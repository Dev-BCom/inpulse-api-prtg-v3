# utils/file_utils.py

import os
import json
import shutil
import tarfile
import brotli
from datetime import datetime, date, timedelta

from utils.config import get_config

config = get_config()

class CustomJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        def list_replacer(o):
            if isinstance(o, list):
                return json.dumps(o, ensure_ascii=False, separators=(',', ': '))
            if isinstance(o, dict):
                return {k: list_replacer(v) for k, v in o.items()}
            return o
        obj = list_replacer(obj)
        return super().encode(obj)

async def save_data_and_compress(sensor_id, data):
    base_data_dir = config['data']['base_directory']
    sensor_dir = os.path.join(base_data_dir, str(sensor_id))

    # Ensure the sensor directory exists
    os.makedirs(sensor_dir, exist_ok=True)

    # Organize data by day
    histdata = data.get('histdata', [])
    data_by_day = {}
    for item in histdata:
        datetime_str = item['datetime']
        # Extract the date part
        if ' - ' in datetime_str:
            # For ranges like "8/7/2024 8:30:00 PM - 8:35:00 PM"
            start_datetime_str, _ = datetime_str.split(' - ')
        else:
            start_datetime_str = datetime_str

        try:
            start_datetime = datetime.strptime(start_datetime_str, '%m/%d/%Y %I:%M:%S %p')
        except ValueError:
            try:
                # Try alternative format
                start_datetime = datetime.strptime(start_datetime_str, '%m/%d/%Y %H:%M:%S')
            except ValueError:
                logging.warning(f"Failed to parse datetime: {datetime_str}")
                continue

        day_str = start_datetime.strftime('%Y-%m-%d')

        data_by_day.setdefault(day_str, []).append(item)

    today_str = date.today().strftime('%Y-%m-%d')
    yesterday_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    for day, items in data_by_day.items():
        day_dir = os.path.join(sensor_dir, day)
        if day == today_str:
            # Save data in today's directory
            os.makedirs(day_dir, exist_ok=True)
            data_file = os.path.join(day_dir, 'data.json')
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2, separators=(',', ': '), cls=CustomJSONEncoder)
        else:
            # Save data and compress the directory
            os.makedirs(day_dir, exist_ok=True)
            data_file = os.path.join(day_dir, 'data.json')
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2, separators=(',', ': '), cls=CustomJSONEncoder)

            # Compress the directory if it's not yesterday
            if day != yesterday_str:
                await compress_and_cleanup(day_dir)

    # Compress yesterday's data if the day has changed
    if not is_same_day():
        yesterday_dir = os.path.join(sensor_dir, yesterday_str)
        if os.path.exists(yesterday_dir):
            await compress_and_cleanup(yesterday_dir)

async def compress_and_cleanup(day_dir):
    # Compress the directory into .tar.br
    dir_name = os.path.basename(day_dir)
    parent_dir = os.path.dirname(day_dir)
    tar_file_path = os.path.join(parent_dir, f"{dir_name}.tar")
    br_file_path = f"{tar_file_path}.br"

    # Create tar archive
    with tarfile.open(tar_file_path, "w") as tar:
        tar.add(day_dir, arcname=os.path.basename(day_dir))

    # Compress with Brotli
    with open(tar_file_path, 'rb') as f_in:
        compressed_data = brotli.compress(f_in.read())
    with open(br_file_path, 'wb') as f_out:
        f_out.write(compressed_data)

    # Remove the tar file and the original directory
    os.remove(tar_file_path)
    shutil.rmtree(day_dir)

def is_same_day():
    # Placeholder function
    # Implement actual logic to check if it's the same day as the last run
    return True
