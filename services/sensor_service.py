# services/sensor_service.py

import asyncio
import logging
from datetime import datetime, timedelta, timezone, time
from time import perf_counter
import sys
from contextlib import redirect_stdout, redirect_stderr

from utils.config import get_config
from utils.db_utils import get_sensors, update_import_filled_until
from utils.api_utils import get_device_info, get_prtg_data
from utils.interval_utils import group_data_into_intervals
from utils.file_utils import save_data_and_compress, save_data_scheduler

from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from asyncio import Lock

# Initialize logger
logger = logging.getLogger(__name__)

# Load configuration
config = get_config()

# Initialize Rich Console
console = Console()

# Maximum number of concurrent sensors to process (Main Process)
max_concurrent_sensors = config.get('max_concurrent_sensors', 10)

# Scheduler configurations
scheduler_request_limit = config['scheduler']['request_limit']  # 100
scheduler_max_workers = config['scheduler'].get('max_concurrent_workers', 100)
scheduler_semaphore = asyncio.Semaphore(scheduler_request_limit)  # Limits to 100 concurrent scheduler requests

# Configuration Parameter for Fetch Interval
fetch_interval_minutes = config['scheduler'].get('fetch_interval_minutes', 20)  # Default to 20 minutes

async def process_sensors(overwrite=False):
    """
    Processes sensors by fetching device info and PRTG data.

    Args:
        overwrite (bool): If True, existing files will be overwritten.
    """
    sensors = await get_sensors()
    total_sensors = len(sensors)
    logger.info(f"Found {total_sensors} sensors to process.")

    if total_sensors == 0:
        logger.info("No sensors to process. Exiting.")
        return

    # Initialize Rich Progress without transient
    progress = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        refresh_per_second=5,
    )

    # Shared state for active requests (Main Process)
    active_requests = {'count': 0}
    active_requests_lock = Lock()
    active_requests_task = progress.add_task("Active Requests: 0", total=1)

    # Task for total sensors, include total number in description
    total_task = progress.add_task(f"[bold green]Total Sensors ({total_sensors})", total=total_sensors)

    # Shared state for worker tasks (Main Process)
    worker_tasks = {}
    for i in range(max_concurrent_sensors):
        worker_task_id = progress.add_task(f"Worker {i}", total=1)
        worker_tasks[i] = worker_task_id

    # Populate the sensor queue
    sensor_queue = asyncio.Queue()
    for sensor in sensors:
        await sensor_queue.put(sensor)

    sensor_times = []

    async def worker(worker_id):
        worker_task_id = worker_tasks[worker_id]
        while True:
            sensor = await sensor_queue.get()
            if sensor is None:
                sensor_queue.task_done()
                break
            await process_sensor(
                sensor,
                progress,
                total_task,
                active_requests,
                active_requests_lock,
                active_requests_task,
                sensor_times,
                worker_id,
                worker_task_id,
                overwrite
            )
            sensor_queue.task_done()

    # Suppress console logging during progress display
    with progress:
        with redirect_stdout(sys.__stdout__), redirect_stderr(sys.__stderr__):
            # Create worker tasks (Main Process)
            workers = [asyncio.create_task(worker(i)) for i in range(max_concurrent_sensors)]
            # Wait until all sensors are processed
            await sensor_queue.join()
            # Stop workers
            for _ in workers:
                await sensor_queue.put(None)
            await asyncio.gather(*workers, return_exceptions=True)

            # Ensure total sensors task is marked as complete
            progress.update(total_task, completed=total_sensors)

    # Calculate and log average processing time
    if sensor_times:
        avg_time = sum(sensor_times) / len(sensor_times)
        logger.info(f"Average processing time per sensor: {avg_time:.2f} seconds")

async def process_sensor(sensor, progress, total_task, active_requests, active_requests_lock, active_requests_task, sensor_times, worker_id, worker_task_id, overwrite):
    start_time = perf_counter()
    sensor_id = sensor['api_id']
    parent_id = sensor['parent_id']
    import_filled_until = sensor['import_filled_until']
    import_start_date = sensor['import_start_date']

    # Update the worker's task
    progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: Checking if processing is needed", total=1, completed=0)

    if parent_id is None:
        logger.warning(f"Sensor {sensor_id} has no parent_id, skipping device info request.")
        progress.advance(total_task, advance=1)
        return

    if import_filled_until:
        date_after_dt = import_filled_until  # Assume naive datetime
    elif import_start_date:
        date_after_dt = import_start_date  # Assume naive datetime
    else:
        # logger.warning(f"Sensor {sensor_id} has no valid date for import_filled_until or import_start_date. Skipping...")
        progress.advance(total_task, advance=1)
        return

    date_after = int(date_after_dt.timestamp())

    try:
        # Fetch device info
        progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: Getting device info", total=1, completed=0)
        device_info = await get_device_info(parent_id, date_after)
        if not device_info:
            logger.warning(f"No device info found for sensor {sensor_id}. Skipping further processing.")
            progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [red]No device info", completed=1)
            progress.advance(total_task, advance=1)
            # Update import_filled_until with cache_last_updated
            await update_import_filled_until(sensor['id'], datetime.utcnow())
            return

        # Extract cache_last_updated and device_data_list
        cache_last_updated_str = device_info.get('cache_last_updated')
        device_data_list = device_info.get('data', [])

        if not cache_last_updated_str:
            logger.warning(f"No cache_last_updated in device_info for sensor {sensor_id}")
            cache_last_updated = datetime.utcnow()
        else:
            try:
                cache_last_updated = datetime.fromisoformat(cache_last_updated_str)
            except ValueError:
                logger.warning(f"Invalid cache_last_updated format: {cache_last_updated_str} for sensor {sensor_id}")
                cache_last_updated = datetime.utcnow()

        # Group data into intervals
        import_start_date_dt = date_after_dt
        intervals = group_data_into_intervals(device_data_list, import_start_date_dt, max_interval_days=7)
        num_intervals = len(intervals)
        logger.info(f"Sensor {sensor_id}: Generated {num_intervals} intervals.")

        if num_intervals == 0:
            logger.info(f"Sensor {sensor_id}: No intervals to process.")
            progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [red]No intervals", completed=1)
            progress.advance(total_task, advance=1)
            # Always update import_filled_until with cache_last_updated
            await update_import_filled_until(sensor['id'], cache_last_updated)
            return

        # Update task to reflect the number of intervals
        progress.update(worker_task_id, total=num_intervals, completed=0)

        for idx, interval in enumerate(intervals, start=1):
            # Update description for current interval
            progress.update(
                worker_task_id,
                description=f"Worker {worker_id}: Sensor {sensor_id}: Processing interval {idx}/{num_intervals}"
            )
            await process_interval(
                sensor,
                interval,
                progress,
                active_requests,
                active_requests_lock,
                active_requests_task,
                cache_last_updated,  # Pass cache_last_updated
                overwrite
            )
            # Advance the progress bar
            progress.advance(worker_task_id)

        logger.info(f"Finished processing for sensor {sensor_id}")
        progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [bold green]Done", completed=progress.tasks[worker_task_id].total)
        # Update the total sensors task
        progress.advance(total_task, advance=1)
        # Always update import_filled_until with cache_last_updated
        await update_import_filled_until(sensor['id'], cache_last_updated)
    except Exception as e:
        logger.error(f"Error processing sensor {sensor_id}: {e}")
        progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [red]Error", completed=1)
        progress.advance(total_task, advance=1)
        # Always update import_filled_until with cache_last_updated in case of error
        await update_import_filled_until(sensor['id'], cache_last_updated)
    finally:
        end_time = perf_counter()
        elapsed_time = end_time - start_time
        sensor_times.append(elapsed_time)

async def process_interval(sensor, interval, progress, active_requests, active_requests_lock, active_requests_task, cache_last_updated, overwrite):
    sensor_id = sensor['api_id']
    start_date, end_date = interval

    # Start date time is start_date at time 00:00:00
    start_date_dt = datetime.combine(start_date, time.min)

    # End date time is end_date at time 23:59:59, unless it's after 'cache_last_updated'
    end_date_dt = datetime.combine(end_date, time.max)
    if end_date_dt > cache_last_updated:
        end_date_dt = cache_last_updated

    start_date_str = start_date_dt.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_date_dt.strftime("%Y-%m-%d-%H-%M-%S")

    # Increment active requests
    async with active_requests_lock:
        active_requests['count'] += 1
        progress.update(active_requests_task, description=f"Active Requests: {active_requests['count']}")

    try:
        # Fetch PRTG data
        data = await get_prtg_data(sensor_id, start_date_str, end_date_str)

        if data:
            # Save and compress the data
            if overwrite:
                await save_data_and_compress(sensor_id, data, overwrite=True)
            else:
                await save_data_scheduler(sensor_id, data)
        # else:
        #     logger.error(f"No data received for sensor {sensor_id} in interval {start_date_str} to {end_date_str}")
    except Exception as e:
        logger.error(f"Error processing interval for sensor {sensor_id}: {e}")
    finally:
        # Decrement active requests
        async with active_requests_lock:
            active_requests['count'] -= 1
            progress.update(active_requests_task, description=f"Active Requests: {active_requests['count']}")

async def scheduled_process_sensors():
    """
    Scheduler that runs every 5 minutes to fetch the last x minutes of data from PRTG.
    """
    while True:
        logger.info("Scheduler: Starting scheduled sensor processing.")
        try:
            await process_sensors_scheduled()
        except Exception as e:
            logger.error(f"Scheduler: Error during scheduled sensor processing: {e}")
        logger.info("Scheduler: Sleeping for 5 minutes.")
        await asyncio.sleep(300)  # Sleep for 5 minutes (300 seconds)

async def process_sensors_scheduled():
    """
    Processes sensors by fetching the last x minutes of PRTG data without affecting import_filled_until.
    """
    sensors = await get_sensors()
    total_sensors = len(sensors)
    logger.info(f"Scheduler: Found {total_sensors} sensors to process.")

    if total_sensors == 0:
        logger.info("Scheduler: No sensors to process. Exiting.")
        return

    # Initialize Rich Progress without transient
    progress = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        refresh_per_second=5,
    )

    # Task for total sensors
    total_task = progress.add_task(f"[bold blue]Total Sensors ({total_sensors}) [Scheduler]", total=total_sensors)

    # Populate the sensor queue
    sensor_queue = asyncio.Queue()
    for sensor in sensors:
        await sensor_queue.put(sensor)

    sensor_times = []

    async def worker():
        while True:
            sensor = await sensor_queue.get()
            if sensor is None:
                sensor_queue.task_done()
                break
            await process_sensor_scheduled(
                sensor,
                progress,
                total_task,
                sensor_times
            )
            sensor_queue.task_done()

    # Suppress console logging during progress display
    with progress:
        with redirect_stdout(sys.__stdout__), redirect_stderr(sys.__stderr__):
            # Create worker tasks (Scheduler)
            workers = [asyncio.create_task(worker()) for _ in range(scheduler_max_workers)]
            # Wait until all sensors are processed
            await sensor_queue.join()
            # Stop workers
            for _ in workers:
                await sensor_queue.put(None)
            await asyncio.gather(*workers, return_exceptions=True)

            # Ensure total sensors task is marked as complete
            progress.update(total_task, completed=total_sensors)

    # Calculate and log average processing time
    if sensor_times:
        avg_time = sum(sensor_times) / len(sensor_times)
        logger.info(f"Scheduler: Average processing time per sensor: {avg_time:.2f} seconds")

async def process_sensor_scheduled(sensor, progress, total_task, sensor_times):
    start_time = perf_counter()
    sensor_id = sensor['api_id']

    # Update total progress
    progress.update(total_task, description=f"Processing Sensor {sensor_id}", advance=1)

    try:
        # Calculate timestamp for x minutes ago
        fetch_interval = fetch_interval_minutes  # Already loaded from config
        end_time = datetime.utcnow()
        start_time_interval = end_time - timedelta(minutes=fetch_interval)

        # Create a single interval tuple
        intervals = [(start_time_interval, end_time)]
        num_intervals = len(intervals)
        logger.debug(f"Scheduler: Sensor {sensor_id}: Generated {num_intervals} intervals for last {fetch_interval_minutes} minutes.")

        if num_intervals == 0:
            logger.debug(f"Scheduler: Sensor {sensor_id}: No intervals to process.")
            return

        for interval in intervals:
            await process_interval_scheduled(
                sensor,
                interval,
                progress
            )

        logger.debug(f"Scheduler: Finished processing for sensor {sensor_id}")
    except Exception as e:
        logger.error(f"Scheduler: Error processing sensor {sensor_id}: {e}")
    finally:
        end_time_perf = perf_counter()
        elapsed_time = end_time_perf - start_time
        sensor_times.append(elapsed_time)

async def process_interval_scheduled(sensor, interval, progress):
    sensor_id = sensor['api_id']
    start_time_interval, end_time_interval = interval

    start_date_str = start_time_interval.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_time_interval.strftime("%Y-%m-%d-%H-%M-%S")

    try:
        # Acquire semaphore before making the request
        async with scheduler_semaphore:
            # Fetch PRTG data
            data = await get_prtg_data(sensor_id, start_date_str, end_date_str)

            if data:
                # Save and compress the data without affecting import_filled_until
                await save_data_scheduler(sensor_id, data)
            # else:
            #     logger.error(f"Scheduler: No data received for sensor {sensor_id} in interval {start_date_str} to {end_date_str}")
    except Exception as e:
        logger.error(f"Scheduler: Error processing interval for sensor {sensor_id}: {e}")

# Entry point for the service
if __name__ == "__main__":
    asyncio.run(process_sensors(overwrite=True))
