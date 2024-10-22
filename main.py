from fastapi import FastAPI
from services.sensor_service import process_sensors

app = FastAPI()

@app.get("/process-sensors")
async def process_sensors_endpoint():
    await process_sensors()
    return {"message": "Processing started"}
