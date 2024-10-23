# utils/api_utils.py

import time
import requests
import logging
import asyncio
import random  # For jitter
from utils.config import get_config
from utils.data_utils import parse_prtg_response  # Updated below

config = get_config()

# Retry parameters
MAX_RETRIES = 5  # Increased retries
INITIAL_RETRY_DELAY = 2  # Initial retry delay in seconds
MAX_RETRY_DELAY = 60  # Maximum delay between retries

# Semaphore to limit concurrent requests to the device-info API
device_info_semaphore = asyncio.Semaphore(config['api']['max_concurrent_device_info_requests'])

def get_prtg_data_sync(sensor_id, sdate, edate):
    """
    Synchronous function to fetch data from the PRTG API for a given sensor within a specific time interval.
    """
    prtg_url = config['api']['prtg_url']
    apitoken = config['api']['apitoken']
    url = f"{prtg_url}?id={sensor_id}&sdate={sdate}&edate={edate}&avg=0&apitoken={apitoken}"

    retries = 0
    retry_delay = INITIAL_RETRY_DELAY

    while retries < MAX_RETRIES:
        try:
            logging.info(f"Making request to PRTG API: {url}")
            headers = {'Accept-Encoding': 'identity'}  # Request uncompressed data
            response = requests.get(url, headers=headers, stream=True, timeout=None)

            if response.status_code == 200:
                # Read the content without enforcing Content-Length
                content = b''.join(response.iter_content(chunk_size=8192))
                text = content.decode('utf-8', errors='replace')
                data = parse_prtg_response(text)
                logging.info(f"PRTG API request successful for sensor {sensor_id} on URL: {url}")
                return data
            else:
                logging.warning(
                    f"Received non-200 response: {response.status_code} for sensor_id {sensor_id} on URL: {url}"
                )
                # Decide to retry on any non-200 response
        except requests.exceptions.RequestException as e:
            logging.warning(
                f"RequestException for sensor {sensor_id} on URL: {url}, Error: {e}. "
                f"Retrying in {retry_delay} seconds... ({retries + 1}/{MAX_RETRIES})"
            )
        except Exception as e:
            logging.error(f"Unexpected error for sensor {sensor_id} on URL: {url}, Error: {e}")
            return None

        retries += 1
        # Implement exponential backoff with jitter
        sleep_time = min(retry_delay * (2 ** retries), MAX_RETRY_DELAY)
        sleep_time += random.uniform(0, 1)  # Add jitter
        logging.info(f"Retrying in {sleep_time:.2f} seconds... ({retries}/{MAX_RETRIES})")
        time.sleep(sleep_time)

    logging.error(
        f"Failed to retrieve data from PRTG API for sensor {sensor_id} after {MAX_RETRIES} retries on URL: {url}"
    )
    return None

def get_device_info_sync(device_id, date_after):
    """
    Synchronous function to fetch device information for a given device ID after a specific timestamp.
    """
    if device_id is None:
        logging.warning(f"device_id is None, skipping device-info request.")
        return None

    base_url = config['api']['base_url']
    url = f"{base_url}/device-info?device_id={device_id}&date_after={date_after}"

    retries = 0
    retry_delay = INITIAL_RETRY_DELAY

    while retries < MAX_RETRIES:
        try:
            logging.info(f"Making request to device-info API: {url}")
            response = requests.get(url, timeout=None)

            if response.status_code == 200:
                data = response.json()
                logging.info(f"Device info request successful for device_id {device_id}")
                return data
            else:
                logging.warning(
                    f"Received non-200 response: {response.status_code} for device_id {device_id} on URL: {url}"
                )
                # Decide to retry on any non-200 response
        except requests.exceptions.RequestException as e:
            logging.warning(
                f"RequestException for device_id {device_id} on URL: {url}, Error: {e}. "
                f"Retrying in {retry_delay} seconds... ({retries + 1}/{MAX_RETRIES})"
            )
        except Exception as e:
            logging.error(f"Unexpected error for device_id {device_id} on URL: {url}, Error: {e}")
            return None

        retries += 1
        # Implement exponential backoff with jitter
        sleep_time = min(retry_delay * (2 ** retries), MAX_RETRY_DELAY)
        sleep_time += random.uniform(0, 1)  # Add jitter
        logging.info(f"Retrying in {sleep_time:.2f} seconds... ({retries}/{MAX_RETRIES})")
        time.sleep(sleep_time)

    logging.error(
        f"Failed to retrieve data from device-info API for device_id {device_id} after {MAX_RETRIES} retries on URL: {url}"
    )
    return None

async def get_prtg_data(sensor_id, sdate, edate):
    """
    Asynchronous wrapper for get_prtg_data_sync
    """
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, get_prtg_data_sync, sensor_id, sdate, edate)
    return data

async def get_device_info(device_id, date_after):
    """
    Asynchronous wrapper for get_device_info_sync
    """
    async with device_info_semaphore:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, get_device_info_sync, device_id, date_after)
    return data

# Ensure that parse_prtg_response remains correctly implemented as per your needs.
