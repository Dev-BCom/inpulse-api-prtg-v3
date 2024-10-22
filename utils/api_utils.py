import aiohttp
from utils.config import get_config

config = get_config()

async def get_device_info(device_id, date_after):
    base_url = config['api']['base_url']
    url = f"{base_url}/device-info?device_id={device_id}&date_after={date_after}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return data

async def get_prtg_data(sensor_id, sdate, edate):
    prtg_url = config['api']['prtg_url']
    apitoken = config['api']['apitoken']
    url = f"{prtg_url}?id={sensor_id}&sdate={sdate}&edate={edate}&apitoken={apitoken}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return data
