# services/sensor_service.py

import asyncio
import logging
from datetime import datetime
from time import perf_counter

from utils.config import get_config
from utils.db_utils import get_sensors, update_import_filled_until
from utils.api_utils import get_device_info, get_prtg_data
from utils.interval_utils import group_data_into_intervals
from utils.file_utils import save_data_and_compress
from utils.data_utils import process_prtg_data

# Import Rich modules
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
from rich.logging import RichHandler

from asyncio import Lock

config = get_config()

# Set up Rich console and logging
console = Console()

# Configure logging to use RichHandler
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, show_path=False)]
)

async def process_sensors():
    sensors = await get_sensors()
    total_sensors = len(sensors)
    logging.info(f"Found {total_sensors} sensors to process.")

    if total_sensors == 0:
        logging.info("No sensors to process. Exiting.")
        return

    # Semaphore to limit concurrent PRTG API requests
    prtg_semaphore = asyncio.Semaphore(config['prtg']['max_concurrent_requests'])

    # Set up Rich progress
    progress = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,  # Keeps the progress bars visible
    )

    # Initialize active requests counter and lock
    active_requests = {'count': 0}
    active_requests_lock = Lock()
    active_requests_task = progress.add_task("Active Requests: 0", total=1)

    # Create a task for total sensors
    total_task = progress.add_task("[bold green]Total Sensors", total=total_sensors)

    # Create a dictionary to hold sensor tasks
    sensor_tasks = {}
    sensor_times = []

    with progress:
        tasks = []
        for sensor in sensors:
            sensor_id = sensor['api_id']
            sensor_task_id = progress.add_task(f"Sensor {sensor_id}", total=1)
            sensor_tasks[sensor_id] = sensor_task_id
            task = asyncio.create_task(
                process_sensor(
                    sensor,
                    prtg_semaphore,
                    progress,
                    sensor_task_id,
                    active_requests,
                    active_requests_lock,
                    active_requests_task,
                    sensor_times
                )
            )
            tasks.append(task)
        await asyncio.gather(*tasks)
        progress.update(total_task, completed=total_sensors)

    # Calculate average processing time
    if sensor_times:
        avg_time = sum(sensor_times) / len(sensor_times)
        logging.info(f"Average processing time per sensor: {avg_time:.2f} seconds")

async def process_sensor(sensor, prtg_semaphore, progress, sensor_task_id, active_requests, active_requests_lock, active_requests_task, sensor_times):
    """
    This function processes a sensor's intervals sequentially (oldest to newest)
    but processes multiple sensors concurrently.
    """
    start_time = perf_counter()
    sensor_id = sensor['api_id']
    parent_id = sensor['parent_id']
    import_filled_until = sensor['import_filled_until']
    import_start_date = sensor['import_start_date']

    if parent_id is None:
        logging.warning(f"Sensor {sensor_id} has no parent_id, skipping device info request.")
        progress.update(sensor_task_id, description=f"[red]Sensor {sensor_id} skipped")
        progress.update(sensor_task_id, completed=1)
        return  # Skip this sensor if parent_id is None

    date_after = None
    if import_filled_until:
        date_after = int(import_filled_until.timestamp())
    elif import_start_date:
        date_after = int(import_start_date.timestamp())
    else:
        logging.warning(f"Sensor {sensor_id} has no valid date for import_filled_until or import_start_date. Skipping...")
        progress.update(sensor_task_id, description=f"[red]Sensor {sensor_id} skipped")
        progress.update(sensor_task_id, completed=1)
        return

    # Convert import_start_date to datetime object for interval grouping
    if import_start_date:
        import_start_date_dt = import_start_date
    else:
        import_start_date_dt = datetime.fromtimestamp(date_after)

    # Step 1: Get device info
    progress.update(sensor_task_id, description=f"Sensor {sensor_id}: Getting device info")
    device_info = await get_device_info(parent_id, date_after)
    if not device_info:
        logging.warning(f"No device info found for sensor {sensor_id}. Skipping further processing.")
        progress.update(sensor_task_id, description=f"[red]Sensor {sensor_id}: No device info")
        progress.update(sensor_task_id, completed=1)
        return

    # Step 2: Process data into intervals
    intervals = group_data_into_intervals(device_info, import_start_date_dt)
    num_intervals = len(intervals)
    logging.info(f"Sensor {sensor_id}: Generated {num_intervals} intervals.")

    if num_intervals == 0:
        logging.info(f"Sensor {sensor_id}: No intervals to process.")
        progress.update(sensor_task_id, description=f"Sensor {sensor_id}: [red]No intervals")
        progress.update(sensor_task_id, completed=1)
        return

    # Update task total to number of intervals
    progress.update(sensor_task_id, total=num_intervals, completed=0)

    # Step 3: Process intervals sequentially (oldest to newest)
    for idx, interval in enumerate(intervals, start=1):
        progress.update(
            sensor_task_id,
            description=f"Sensor {sensor_id}: Processing interval {idx}/{num_intervals}"
        )
        await process_interval(
            sensor,
            interval,
            prtg_semaphore,
            progress,
            active_requests,
            active_requests_lock,
            active_requests_task
        )
        progress.advance(sensor_task_id, advance=1)

    logging.info(f"Finished processing for sensor {sensor_id}")
    progress.update(sensor_task_id, description=f"Sensor {sensor_id}: [bold green]Done")
    progress.refresh()

    end_time = perf_counter()
    elapsed_time = end_time - start_time
    sensor_times.append(elapsed_time)

async def process_interval(sensor, interval, prtg_semaphore, progress, active_requests, active_requests_lock, active_requests_task):
    """
    Process an individual interval for a sensor, respecting the semaphore.
    """
    sensor_id = sensor['api_id']
    start_date, end_date = interval
    start_date_str = start_date.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_date.strftime("%Y-%m-%d-%H-%M-%S")

    # Increment active requests
    async with active_requests_lock:
        active_requests['count'] += 1
        progress.update(active_requests_task, description=f"Active Requests: {active_requests['count']}")

    logging.info(f"Sensor {sensor_id}: Requesting data from {start_date_str} to {end_date_str}")

    # Call PRTG API using get_prtg_data
    data = await get_prtg_data(sensor_id, start_date_str, end_date_str)
    active_requests_count = active_requests['count']
    logging.info(f"Active requests: {active_requests_count}")

    if data:
        # Process and save data
        processed_data = process_prtg_data(data)
        await save_data_and_compress(sensor_id, processed_data)
    else:
        logging.error(f"No data received for sensor {sensor_id} in interval {start_date_str} to {end_date_str}")

    # Decrement active requests
    async with active_requests_lock:
        active_requests['count'] -= 1
        progress.update(active_requests_task, description=f"Active Requests: {active_requests['count']}")

    # Step 4: Update 'import_filled_until' in the database (after processing the interval)
    await update_import_filled_until(sensor['id'], end_date)
