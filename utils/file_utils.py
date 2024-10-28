import os
import json
import shutil
import tarfile
import brotli
from datetime import datetime, timedelta

from utils.config import get_config
import aiofiles
import aiofiles.os
import asyncio

config = get_config()

async def save_data_and_compress(sensor_id, data):
    """
    Saves sensor data into JSON files organized by day and compresses past days' data.

    Args:
        sensor_id (str): The unique identifier for the sensor.
        data (dict): The data payload containing 'histdata'.
    """
    base_data_dir = config['data']['base_directory']
    sensor_dir = os.path.join(base_data_dir, str(sensor_id))

    # Ensure the sensor directory exists asynchronously
    await ensure_directory(sensor_dir)

    # Organize data by day
    histdata = data.get('histdata', [])
    data_by_day = {}
    for item in histdata:
        datetime_str = item.get('datetime', '')
        # Extract the start date part
        if ' - ' in datetime_str:
            # For ranges like "8/7/2024 8:30:00 PM - 8:35:00 PM"
            start_datetime_str, _ = datetime_str.split(' - ', 1)
        else:
            start_datetime_str = datetime_str

        try:
            # Parse the datetime string into a datetime object
            start_datetime = datetime.strptime(start_datetime_str, '%m/%d/%Y %I:%M:%S %p')
        except ValueError:
            try:
                # Try alternative format without AM/PM
                start_datetime = datetime.strptime(start_datetime_str, '%m/%d/%Y %H:%M:%S')
            except ValueError:
                continue

        day_str = start_datetime.strftime('%Y-%m-%d')
        data_by_day.setdefault(day_str, []).append(item)

    # Get today's date in UTC
    today_str = datetime.utcnow().strftime('%Y-%m-%d')

    # Prepare compression tasks for past days
    compression_tasks = []

    for day, items in data_by_day.items():
        day_dir = os.path.join(sensor_dir, day)
        await ensure_directory(day_dir)

        data_file = os.path.join(day_dir, 'data.json')
        try:
            async with aiofiles.open(data_file, 'w', encoding='utf-8') as f:
                json_str = json.dumps(items, ensure_ascii=False, indent=2, separators=(',', ': '))
                await f.write(json_str)
        except TypeError:
            continue

        if day != today_str:
            # Schedule compression and cleanup for past days
            compression_tasks.append(compress_and_cleanup(day_dir))

    if compression_tasks:
        # Run all compression tasks concurrently
        await asyncio.gather(*compression_tasks)


async def ensure_directory(path):
    """
    Asynchronously ensures that a directory exists.

    Args:
        path (str): The directory path to ensure.
    """
    try:
        await aiofiles.os.makedirs(path, exist_ok=True)
    except AttributeError:
        # Fallback if aiofiles.os.makedirs does not support exist_ok
        await asyncio.to_thread(os.makedirs, path, exist_ok=True)
    except Exception:
        pass


async def compress_and_cleanup(day_dir):
    """
    Compresses the specified directory into a .tar.br file and removes the original directory.

    Args:
        day_dir (str): The directory path to compress and clean up.
    """
    # Define file paths
    dir_name = os.path.basename(day_dir)
    parent_dir = os.path.dirname(day_dir)
    tar_file_path = os.path.join(parent_dir, f"{dir_name}.tar")
    br_file_path = f"{tar_file_path}.br"

    try:
        # Create tar archive asynchronously
        await asyncio.to_thread(create_tar_archive, day_dir, tar_file_path)

        # Compress with Brotli asynchronously
        await asyncio.to_thread(compress_brotli, tar_file_path, br_file_path)

        # Remove the tar file and the original directory asynchronously
        await asyncio.to_thread(os.remove, tar_file_path)
        await asyncio.to_thread(shutil.rmtree, day_dir)
    except Exception:
        pass


def create_tar_archive(source_dir, tar_file_path):
    """
    Creates a tar archive of the specified source directory.

    Args:
        source_dir (str): The directory to archive.
        tar_file_path (str): The destination tar file path.
    """
    try:
        with tarfile.open(tar_file_path, "w") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))
    except Exception:
        raise


def compress_brotli(tar_file_path, br_file_path):
    """
    Compresses the specified tar file using Brotli compression.

    Args:
        tar_file_path (str): The source tar file path.
        br_file_path (str): The destination Brotli file path.
    """
    try:
        with open(tar_file_path, 'rb') as f_in:
            compressed_data = brotli.compress(f_in.read())
        with open(br_file_path, 'wb') as f_out:
            f_out.write(compressed_data)
    except Exception:
        raise