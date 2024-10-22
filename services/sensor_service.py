import asyncio
import json
import logging
from tqdm.asyncio import tqdm_asyncio  # Use tqdm_asyncio for async tasks

from aiomysql import DictCursor
from utils.db_utils import get_pool, update_import_filled_until
from utils.config import get_config
from utils.api_utils import get_device_info, get_prtg_data
from utils.data_processing import group_data_into_intervals
from utils.active_request_counter import ActiveRequestCounter

config = get_config()
logging.basicConfig(level=logging.INFO)

async def process_sensors():
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute("""
                SELECT * FROM api_connections
                WHERE (api_data_type NOT IN ('link', 'device', 'group') OR api_data_type IS NULL)
                AND api = 'prtg'
                AND api_needs_connection_checking = 'n'
                AND api_connected = 'y'
            """)
            sensors = await cur.fetchall()

    total_sensors = len(sensors)
    logging.info(f"Found {total_sensors} sensors to process.")

    active_request_counter = ActiveRequestCounter(config['prtg']['max_concurrent_requests'])

    # Use tqdm_asyncio for async tasks
    tasks = [process_sensor(sensor, active_request_counter) for sensor in sensors]
    for _ in tqdm_asyncio.as_completed(tasks, total=total_sensors, desc="Processing Sensors"):
        await _

async def process_sensor(sensor, active_request_counter):
    sensor_id = sensor['api_id']
    parent_id = sensor['parent_id']
    import_filled_until = sensor['import_filled_until']
    import_start_date = sensor['import_start_date']

    logging.info(f"Starting processing for sensor {sensor_id} with parent_id {parent_id}")

    date_after = None
    if import_filled_until:
        date_after = int(import_filled_until.timestamp())
        logging.info(f"Using import_filled_until: {import_filled_until} as date_after")
    elif import_start_date:
        date_after = int(import_start_date.timestamp())
        logging.info(f"Using import_start_date: {import_start_date} as date_after")
    else:
        logging.warning(f"Sensor {sensor_id} has no valid date for import_filled_until or import_start_date. Skipping...")
        return

    # Step 1: Get device info
    device_info = await get_device_info(parent_id, date_after)
    logging.info(f"Retrieved device info for sensor {sensor_id}: {len(device_info)} items")

    if not device_info:
        logging.info(f"No device info found for sensor {sensor_id}. Skipping further processing.")
        return

    # Step 2: Process data into intervals
    intervals = group_data_into_intervals(device_info)
    logging.info(f"Generated {len(intervals)} intervals for sensor {sensor_id}")

    # Step 3: For each interval, call PRTG API
    tasks = [process_interval(sensor, interval, active_request_counter) for interval in intervals]
    await asyncio.gather(*tasks)

    logging.info(f"Finished processing for sensor {sensor_id}")

async def process_interval(sensor, interval, active_request_counter):
    sensor_id = sensor['api_id']
    start_date, end_date = interval
    start_date_str = start_date.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_date.strftime("%Y-%m-%d-%H-%M-%S")

    async with active_request_counter:
        logging.info(f"Sending request to PRTG API for sensor {sensor_id}, interval {start_date_str} to {end_date_str}")
        # Call PRTG API
        data = await get_prtg_data(sensor_id, start_date_str, end_date_str)
        logging.info(f"Received data from PRTG API for sensor {sensor_id}, interval {start_date_str} to {end_date_str}")

        # Save data to JSON file
        filename = f"sensor_{sensor_id}_{start_date_str}_{end_date_str}.json"
        with open(filename, 'w') as f:
            json.dump(data, f)
        logging.info(f"Saved data to {filename}")

        # Update 'import_filled_until' in database
        pool = get_pool()
        async with pool.acquire() as conn:
            await update_import_filled_until(conn, sensor['id'], end_date)
        logging.info(f"Updated 'import_filled_until' for sensor {sensor_id} to {end_date}")
