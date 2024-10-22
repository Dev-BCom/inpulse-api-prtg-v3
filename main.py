from fastapi import FastAPI
from contextlib import asynccontextmanager
from services.sensor_service import process_sensors
from utils.db_utils import init_mysql_pool, close_mysql_pool

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event: Initialize MySQL pool
    await init_mysql_pool()
    yield
    # Shutdown event: Close MySQL pool
    await close_mysql_pool()

app = FastAPI(lifespan=lifespan)

@app.get("/process-sensors")
async def process_sensors_endpoint():
    await process_sensors()
    return {"message": "Processing started"}
