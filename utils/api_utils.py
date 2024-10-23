# utils/api_utils.py

import time
import requests
import logging
import asyncio
from utils.config import get_config
from utils.data_utils import parse_prtg_response  # Updated below

config = get_config()

# Retry parameters
MAX_RETRIES = 3
RETRY_DELAY = 2  # Initial retry delay (exponential backoff)

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
    while retries < MAX_RETRIES:
        try:
            # logging.info(f"Making request to PRTG API: {url}")
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
                logging.debug(f"Response content: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            retries += 1
            delay = RETRY_DELAY * (2 ** (retries - 1))
            logging.warning(
                f"RequestException for sensor {sensor_id} on URL: {url}, Error: {e}. "
                f"Retrying in {delay} seconds... ({retries}/{MAX_RETRIES})"
            )
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Unexpected error for sensor {sensor_id} on URL: {url}, Error: {e}")
            return None

    logging.error(
        f"Failed to retrieve data from PRTG API for sensor {sensor_id} after {MAX_RETRIES} retries on URL: {url}"
    )
    return None

async def get_prtg_data(sensor_id, sdate, edate):
    """
    Asynchronous wrapper for get_prtg_data_sync
    """
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, get_prtg_data_sync, sensor_id, sdate, edate)
    return data

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
                logging.debug(f"Response content: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            retries += 1
            delay = RETRY_DELAY * (2 ** (retries - 1))
            logging.warning(
                f"RequestException for device_id {device_id} on URL: {url}, Error: {e}. "
                f"Retrying in {delay} seconds... ({retries}/{MAX_RETRIES})"
            )
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Unexpected error for device_id {device_id} on URL: {url}, Error: {e}")
            return None

    logging.error(
        f"Failed to retrieve data from device-info API for device_id {device_id} after {MAX_RETRIES} retries on URL: {url}"
    )
    return None

async def get_device_info(device_id, date_after):
    """
    Asynchronous wrapper for get_device_info_sync
    """
    async with device_info_semaphore:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, get_device_info_sync, device_id, date_after)
    return data

import re

def parse_prtg_response(text):
    """
    Parses the PRTG API response text and handles duplicate keys by organizing them into arrays.
    """
    # Remove newline characters
    text = text.replace('\n', '')

    # Use regex to find the 'histdata' array
    match = re.search(r'"histdata":\s*(\[\{.*?\}\])', text)
    if not match:
        logging.error("No 'histdata' found in PRTG API response.")
        return {}

    histdata_text = match.group(1)

    # Split the histdata_text into individual items
    # This regex accounts for nested structures and ensures proper splitting
    items_text = re.findall(r'\{([^}]+)\}', histdata_text)
    histdata = []

    for item_text in items_text:
        # Initialize an empty dictionary for each item
        item = {}
        # Prepare a dictionary to collect fields with potential duplicates
        fields = {}
        # Split the item text into key-value pairs
        # This regex handles keys and values with proper quotation marks
        pairs = re.findall(r'"([^"]+)"\s*:\s*(?:"([^"]*)"|([-\d.]+))', item_text)

        for pair in pairs:
            key = pair[0]
            val_str = pair[1] if pair[1] else pair[2]

            # Collect duplicate keys into lists
            if key in fields:
                if isinstance(fields[key], list):
                    fields[key].append(val_str)
                else:
                    fields[key] = [fields[key], val_str]
            else:
                fields[key] = val_str

        # Process fields to convert to appropriate types and handle duplicates
        for key, val in fields.items():
            if key in ['value', 'value_raw']:
                if not isinstance(val, list):
                    val = [val]
                item[key] = val
            else:
                # Attempt to convert to float or keep as string
                try:
                    if '.' in val or 'e' in val.lower():
                        item[key] = float(val)
                    else:
                        item[key] = int(val)
                except ValueError:
                    item[key] = val

        # Post-processing for specific fields
        if 'value_raw' in item:
            new_value_raw = []
            for v in item['value_raw']:
                try:
                    new_value_raw.append(float(v))
                except ValueError:
                    new_value_raw.append(v)
            item['value_raw'] = new_value_raw

        if 'value' in item:
            item['value'] = [v.replace('°', '') for v in item['value']]

        histdata.append(item)

    # Return the parsed data
    return {'histdata': histdata}

def process_prtg_data(data):
    # Placeholder for any additional data processing if needed
    # Currently, we simply return the data as-is
    return data
