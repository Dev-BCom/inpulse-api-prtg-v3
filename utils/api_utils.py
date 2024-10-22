import aiohttp
import logging
import asyncio
from utils.config import get_config
from utils.data_utils import parse_prtg_response

config = get_config()

# Semaphore to limit concurrent requests to the device-info API
device_info_semaphore = asyncio.Semaphore(config['api']['max_concurrent_device_info_requests'])

# Retry parameters
MAX_RETRIES = 5
RETRY_DELAY = 2  # Initial retry delay (exponential backoff)

async def get_device_info(device_id, date_after):
    if device_id is None:
        logging.warning(f"device_id is None, skipping device-info request.")
        return None

    base_url = config['api']['base_url']
    url = f"{base_url}/device-info?device_id={device_id}&date_after={date_after}"
    
    retries = 0
    async with device_info_semaphore:
        while retries < MAX_RETRIES:
            try:
                logging.info(f"Making request to device-info API: {url}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:  # Removed timeout
                        if response.status == 200:
                            data = await response.json()
                            logging.info(f"Device info request successful for device_id {device_id}")
                            return data
                        else:
                            logging.warning(f"Received non-200 response: {response.status} for device_id {device_id} on URL: {url}")
                            return None
            except aiohttp.ClientError as e:
                retries += 1
                delay = RETRY_DELAY * (2 ** retries)
                logging.warning(f"ClientError for device_id {device_id} on URL: {url}, Error: {e}. Retrying in {delay} seconds... ({retries}/{MAX_RETRIES})")
                await asyncio.sleep(delay)

        logging.error(f"Failed to retrieve data from device-info API for device_id {device_id} after {MAX_RETRIES} retries on URL: {url}")
        return None


async def get_prtg_data(sensor_id, sdate, edate):
    prtg_url = config['api']['prtg_url']
    apitoken = config['api']['apitoken']
    url = f"{prtg_url}?id={sensor_id}&sdate={sdate}&edate={edate}&avg=0&apitoken={apitoken}"
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            logging.info(f"Making request to PRTG API: {url}")
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:  # Removed timeout
                    if response.status == 200:
                        text = await response.text()
                        data = parse_prtg_response(text)
                        logging.info(f"PRTG API request successful for sensor {sensor_id} on URL: {url}")
                        return data
                    else:
                        logging.warning(f"Received non-200 response: {response.status} for sensor_id {sensor_id} on URL: {url}")
                        return None
        except aiohttp.ClientPayloadError as e:
            retries += 1
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"ClientPayloadError for sensor {sensor_id} on URL: {url}, Error: {e}. Retrying in {delay} seconds... ({retries}/{MAX_RETRIES})")
            await asyncio.sleep(delay)
        except aiohttp.ClientError as e:
            retries += 1
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"ClientError for sensor {sensor_id} on URL: {url}, Error: {e}. Retrying in {delay} seconds... ({retries}/{MAX_RETRIES})")
            await asyncio.sleep(delay)

    logging.error(f"Failed to retrieve data from PRTG API for sensor {sensor_id} after {MAX_RETRIES} retries on URL: {url}")
    return None
