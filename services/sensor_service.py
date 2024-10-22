import asyncio
import json

import aiomysql
from utils.db_utils import get_connection, update_import_filled_until
from utils.config import get_config
from utils.api_utils import get_device_info, get_prtg_data
from utils.data_processing import group_data_into_intervals
from utils.active_request_counter import ActiveRequestCounter
import logging
from tqdm import tqdm

config = get_config()
logging.basicConfig(level=logging.INFO)

async def process_sensors():
    conn = await get_connection()
    async with conn.cursor(aiomysql.DictCursor) as cur:
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

    progress_bar = tqdm(total=total_sensors)
    active_request_counter = ActiveRequestCounter(config['prtg']['max_concurrent_requests'])

    tasks = []
    for sensor in sensors:
        task = asyncio.create_task(process_sensor(sensor, active_request_counter, conn))
        tasks.append(task)

    for f in asyncio.as_completed(tasks):
        await f
        progress_bar.update(1)

    progress_bar.close()
    conn.close()

async def process_sensor(sensor, active_request_counter, conn):
    sensor_id = sensor['api_id']
    parent_id = sensor['parent_id']
    import_filled_until = sensor['import_filled_until']
    import_start_date = sensor['import_start_date']

    date_after = None
    if import_filled_until:
        date_after = int(import_filled_until.timestamp())
    elif import_start_date:
        date_after = int(import_start_date.timestamp())
    else:
        # Handle case where neither date is available
        logging.warning(f"Sensor {sensor_id} has no import_filled_until or import_start_date.")
        return

    # Step 1: Get device info
    device_info = await get_device_info(parent_id, date_after)

    # Step 2: Process data into intervals
    intervals = group_data_into_intervals(device_info)

    # Step 3: For each interval, call PRTG API
    tasks = []
    for interval in intervals:
        task = asyncio.create_task(process_interval(sensor, interval, active_request_counter, conn))
        tasks.append(task)

    for task in asyncio.as_completed(tasks):
        await task

async def process_interval(sensor, interval, active_request_counter, conn):
    sensor_id = sensor['api_id']
    start_date, end_date = interval
    start_date_str = start_date.strftime("%Y-%m-%d-%H-%M-%S")
    end_date_str = end_date.strftime("%Y-%m-%d-%H-%M-%S")
    async with active_request_counter:
        logging.info(f"Sending request to PRTG API for sensor {sensor_id}, interval {start_date_str} to {end_date_str}")
        # Call PRTG API
        data = await get_prtg_data(sensor_id, start_date_str, end_date_str)
        # Save data to JSON file
        filename = f"sensor_{sensor_id}_{start_date_str}_{end_date_str}.json"
        with open(filename, 'w') as f:
            json.dump(data, f)
        # Update 'import_filled_until' in database
        await update_import_filled_until(conn, sensor['id'], end_date)
