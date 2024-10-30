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
from utils.file_utils import save_data_and_compress

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

# Maximum number of concurrent sensors to process
max_concurrent_sensors = config.get('max_concurrent_sensors', 10)

async def process_sensors():
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

    # Shared state for active requests
    active_requests = {'count': 0}
    active_requests_lock = Lock()
    active_requests_task = progress.add_task("Active Requests: 0", total=1)

    # Task for total sensors, include total number in description
    total_task = progress.add_task(f"[bold green]Total Sensors ({total_sensors})", total=total_sensors)

    # Shared state for worker tasks
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
                worker_task_id
            )
            sensor_queue.task_done()

    # Suppress console logging during progress display
    with progress:
        with redirect_stdout(sys.__stdout__), redirect_stderr(sys.__stderr__):
            # Create worker tasks
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

async def process_sensor(sensor, progress, total_task, active_requests, active_requests_lock, active_requests_task, sensor_times, worker_id, worker_task_id):
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

    current_time_utc = datetime.utcnow()  # Make current time naive

    if import_filled_until:
        date_after_dt = import_filled_until  # Assume naive datetime
    elif import_start_date:
        date_after_dt = import_start_date  # Assume naive datetime
    else:
        logger.warning(f"Sensor {sensor_id} has no valid date for import_filled_until or import_start_date. Skipping...")
        progress.advance(total_task, advance=1)
        return

    # Removed the last 10-minute check here

    date_after = int(date_after_dt.timestamp())

    try:
        # Fetch device info
        progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: Getting device info", total=1, completed=0)
        device_info = await get_device_info(parent_id, date_after)
        if not device_info:
            logger.warning(f"No device info found for sensor {sensor_id}. Skipping further processing.")
            progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [red]No device info", completed=1)
            progress.advance(total_task, advance=1)
            return

        # Group data into intervals
        import_start_date_dt = date_after_dt
        intervals = group_data_into_intervals(device_info, import_start_date_dt, max_interval_days=7)
        num_intervals = len(intervals)
        logger.info(f"Sensor {sensor_id}: Generated {num_intervals} intervals.")

        if num_intervals == 0:
            logger.info(f"Sensor {sensor_id}: No intervals to process.")
            progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [red]No intervals", completed=1)
            progress.advance(total_task, advance=1)
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
                active_requests_task
            )
            # Advance the progress bar
            progress.advance(worker_task_id)

        logger.info(f"Finished processing for sensor {sensor_id}")
        progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [bold green]Done", completed=progress.tasks[worker_task_id].total)
        # Update the total sensors task
        progress.advance(total_task, advance=1)
    except Exception as e:
        logger.error(f"Error processing sensor {sensor_id}: {e}")
        progress.update(worker_task_id, description=f"Worker {worker_id}: Sensor {sensor_id}: [red]Error", completed=1)
        progress.advance(total_task, advance=1)
    finally:
        end_time = perf_counter()
        elapsed_time = end_time - start_time
        sensor_times.append(elapsed_time)

async def process_interval(sensor, interval, progress, active_requests, active_requests_lock, active_requests_task):
    sensor_id = sensor['api_id']
    start_date, end_date = interval
    now_utc = datetime.utcnow()

    # Start date time is start_date at time 00:00:00
    start_date_dt = datetime.combine(start_date, time.min)

    # End date time is end_date at time 23:59:59, unless it's today or in the future
    end_date_dt = datetime.combine(end_date, time.max)
    if end_date_dt > now_utc:
        end_date_dt = now_utc

    start_date_str = start_date_dt.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_date_dt.strftime("%Y-%m-%d-%H-%M-%S")

    # Removed the last 10-minute check here

    # Increment active requests
    async with active_requests_lock:
        active_requests['count'] += 1
        progress.update(active_requests_task, description=f"Active Requests: {active_requests['count']}")

    try:
        # Fetch PRTG data
        data = await get_prtg_data(sensor_id, start_date_str, end_date_str)

        if data:
            # Save and compress the data
            await save_data_and_compress(sensor_id, data)

            # Extract maximum datetime from the data
            max_timestamp = None
            histdata = data.get('histdata', [])
            for item in histdata:
                datetime_str = item.get('datetime', '')
                # Extract the start date part
                if ' - ' in datetime_str:
                    # For ranges like "8/7/2024 8:30:00 PM - 8:35:00 PM"
                    start_datetime_str, _ = datetime_str.split(' - ', 1)
                else:
                    start_datetime_str = datetime_str

                try:
                    # Parse the datetime string into a datetime object
                    start_datetime = datetime.strptime(start_datetime_str, '%m/%d/%Y %I:%M:%S %p')
                except ValueError:
                    try:
                        # Try alternative format without AM/PM
                        start_datetime = datetime.strptime(start_datetime_str, '%m/%d/%Y %H:%M:%S')
                    except ValueError:
                        continue

                # Ensure the datetime is naive
                start_datetime = start_datetime  # Already naive

                if (max_timestamp is None) or (start_datetime > max_timestamp):
                    max_timestamp = start_datetime

            if max_timestamp:
                # Update the import_filled_until timestamp to the max timestamp
                await update_import_filled_until(sensor['id'], max_timestamp)
            else:
                # If no data was retrieved, use the end_date_dt
                await update_import_filled_until(sensor['id'], end_date_dt)
        else:
            logger.error(f"No data received for sensor {sensor_id} in interval {start_date_str} to {end_date_str}")
            # Even if no data is received, update the import_filled_until to end_date_dt to prevent reprocessing
            await update_import_filled_until(sensor['id'], end_date_dt)
    except Exception as e:
        logger.error(f"Error processing interval for sensor {sensor_id}: {e}")
    finally:
        # Decrement active requests
        async with active_requests_lock:
            active_requests['count'] -= 1
            progress.update(active_requests_task, description=f"Active Requests: {active_requests['count']}")

# Entry point for the service
if __name__ == "__main__":
    asyncio.run(process_sensors())
