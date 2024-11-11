# main.py

import logging
import os
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from services.sensor_service import process_sensors, scheduled_process_sensors
from utils.db_utils import init_mysql_pool, close_mysql_pool
from rich.console import Console
from rich.logging import RichHandler
from logging.handlers import RotatingFileHandler
import asyncio

# Load configuration
from utils.config import get_config

config = get_config()

# Initialize Rich Console
console = Console()

# Configure logging **before** creating the FastAPI app
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove all existing handlers to prevent duplication
if logger.hasHandlers():
    logger.handlers.clear()

# Ensure the logs directory exists
log_directory = os.path.join(config.get('data', {}).get('base_directory', '.'), 'logs')
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, 'sensor_service.log')

# File handler for logging to a file with rotation
file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Console handler with RichHandler, set to WARNING to minimize clutter
rich_handler = RichHandler(console=console, show_path=False, markup=True)
rich_handler.setLevel(logging.WARNING)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(rich_handler)

# Adjust Uvicorn's loggers to WARNING to prevent INFO logs from appearing in the console
logging.getLogger("uvicorn").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Create the FastAPI app with lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event: Initialize MySQL pool and start scheduler
    await init_mysql_pool()
    logger.info("Starting scheduler task.")
    scheduler_task = asyncio.create_task(scheduled_process_sensors())
    yield
    # Shutdown event: Close MySQL pool and cancel scheduler
    logger.info("Shutting down scheduler task.")
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        logger.info("Scheduler task cancelled.")
    await close_mysql_pool()

app = FastAPI(lifespan=lifespan)

@app.get("/fill-data")
async def process_sensors_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_sensors, overwrite=True)
    return {"message": "Filling data started!"}
