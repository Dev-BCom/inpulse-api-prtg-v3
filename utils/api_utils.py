import aiohttp
import logging
from utils.config import get_config

config = get_config()

async def get_device_info(device_id, date_after):
    base_url = config['api']['base_url']
    url = f"{base_url}/device-info?device_id={device_id}&date_after={date_after}"
    logging.info(f"Making request to device-info API: {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            logging.info(f"Device info request complete for device_id {device_id}")
            return data

async def get_prtg_data(sensor_id, sdate, edate):
    prtg_url = config['api']['prtg_url']
    apitoken = config['api']['apitoken']
    url = f"{prtg_url}?id={sensor_id}&sdate={sdate}&edate={edate}&apitoken={apitoken}"
    logging.info(f"Making request to PRTG API: {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            logging.info(f"PRTG API request complete for sensor_id {sensor_id}")
            return data
