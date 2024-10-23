# services/sensor_service.py

import asyncio
import logging
from datetime import datetime

from utils.config import get_config
from utils.db_utils import get_sensors, update_import_filled_until
from utils.api_utils import get_device_info, get_prtg_data
from utils.interval_utils import group_data_into_intervals
from utils.file_utils import save_data_and_compress
from utils.data_utils import process_prtg_data

config = get_config()
logging.basicConfig(level=logging.INFO)

async def process_sensors():
    sensors = await get_sensors()
    total_sensors = len(sensors)
    logging.info(f"Found {total_sensors} sensors to process.")

    # Semaphore to limit concurrent PRTG API requests
    prtg_semaphore = asyncio.Semaphore(config['prtg']['max_concurrent_requests'])

    # Create tasks for each sensor
    tasks = [process_sensor(sensor, prtg_semaphore) for sensor in sensors]
    await asyncio.gather(*tasks)

async def process_sensor(sensor, prtg_semaphore):
    sensor_id = sensor['api_id']
    parent_id = sensor['parent_id']
    import_filled_until = sensor['import_filled_until']
    import_start_date = sensor['import_start_date']

    if parent_id is None:
        logging.warning(f"Sensor {sensor_id} has no parent_id, skipping device info request.")
        return  # Skip this sensor if parent_id is None

    date_after = None
    if import_filled_until:
        date_after = int(import_filled_until.timestamp())
    elif import_start_date:
        date_after = int(import_start_date.timestamp())
    else:
        logging.warning(f"Sensor {sensor_id} has no valid date for import_filled_until or import_start_date. Skipping...")
        return

    # Convert import_start_date to datetime object for interval grouping
    if import_start_date:
        import_start_date_dt = import_start_date
    else:
        import_start_date_dt = datetime.fromtimestamp(date_after)

    # Step 1: Get device info
    device_info = await get_device_info(parent_id, date_after)
    if not device_info:
        logging.warning(f"No device info found for sensor {sensor_id}. Skipping further processing.")
        return

    # Step 2: Process data into intervals
    intervals = group_data_into_intervals(device_info, import_start_date_dt)
    logging.info(f"Sensor {sensor_id}: Generated {len(intervals)} intervals.")

    # Process intervals concurrently for this sensor, limited by prtg_semaphore
    interval_tasks = [
        process_interval(sensor, interval, prtg_semaphore)
        for interval in intervals
    ]
    await asyncio.gather(*interval_tasks)

    logging.info(f"Finished processing for sensor {sensor_id}")

async def process_interval(sensor, interval, prtg_semaphore):
    sensor_id = sensor['api_id']
    start_date, end_date = interval
    start_date_str = start_date.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_date.strftime("%Y-%m-%d-%H-%M-%S")

    async with prtg_semaphore:
        logging.info(f"Making request to PRTG API for sensor {sensor_id}, interval {start_date_str} to {end_date_str}")
        
        # Call PRTG API using get_prtg_data
        data = await get_prtg_data(sensor_id, start_date_str, end_date_str)
        logging.info(f"Active requests: {config['prtg']['max_concurrent_requests'] - prtg_semaphore._value}")

        if data:
            # Process and save data
            processed_data = process_prtg_data(data)
            await save_data_and_compress(sensor_id, processed_data)
        else:
            logging.error(f"No data received for sensor {sensor_id} in interval {start_date_str} to {end_date_str}")

    # Update 'import_filled_until' in the database
    await update_import_filled_until(sensor['id'], end_date)
