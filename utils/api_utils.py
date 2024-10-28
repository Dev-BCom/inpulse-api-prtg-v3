# utils/api_utils.py

import asyncio
import logging
import random
from requests_futures.sessions import FuturesSession
from utils.config import get_config
from utils.data_utils import parse_prtg_response

config = get_config()

MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 60

device_info_semaphore = asyncio.Semaphore(config['api']['max_concurrent_device_info_requests'])

session = FuturesSession()

async def get_prtg_data(sensor_id, sdate, edate):
    prtg_url = config['api']['prtg_url']
    apitoken = config['api']['apitoken']
    url = f"{prtg_url}?id={sensor_id}&sdate={sdate}&edate={edate}&avg=0&apitoken={apitoken}"

    retries = 0
    retry_delay = INITIAL_RETRY_DELAY

    while retries < MAX_RETRIES:
        try:
            response = await asyncio.wrap_future(session.get(url, timeout=None))
            if response.status_code == 200:
                data = parse_prtg_response(response.text)
                return data
            else:
                logging.warning(f"Non-200 response: {response.status_code} for sensor {sensor_id}")
        except Exception as e:
            logging.warning(f"Error for sensor {sensor_id}: {e}, retrying... ({retries + 1}/{MAX_RETRIES})")

        retries += 1
        sleep_time = min(retry_delay * (2 ** retries), MAX_RETRY_DELAY) + random.uniform(0, 1)
        await asyncio.sleep(sleep_time)

    logging.error(f"Failed to retrieve data for sensor {sensor_id} after {MAX_RETRIES} retries.")
    return None

async def get_device_info(device_id, date_after):
    if device_id is None:
        logging.warning("device_id is None, skipping request.")
        return None

    base_url = config['api']['base_url']
    url = f"{base_url}/device-info?device_id={device_id}&date_after={date_after}"

    retries = 0
    retry_delay = INITIAL_RETRY_DELAY

    while retries < MAX_RETRIES:
        try:
            async with device_info_semaphore:
                response = await asyncio.wrap_future(session.get(url, timeout=None))
                if response.status_code == 200:
                    return response.json()
                else:
                    logging.warning(f"Non-200 response: {response.status_code} for device_id {device_id}")
        except Exception as e:
            logging.warning(f"Error for device_id {device_id}: {e}, retrying... ({retries + 1}/{MAX_RETRIES})")

        retries += 1
        sleep_time = min(retry_delay * (2 ** retries), MAX_RETRY_DELAY) + random.uniform(0, 1)
        await asyncio.sleep(sleep_time)

    logging.error(f"Failed to retrieve data for device {device_id} after {MAX_RETRIES} retries.")
    return None
