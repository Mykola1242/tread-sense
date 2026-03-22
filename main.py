from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List
from sqlalchemy import insert, select, update, delete
import uvicorn

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)

class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float

class GpsData(BaseModel):
    latitude: float
    longitude: float

class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator('timestamp', mode='before')
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )

class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

app = FastAPI() 

subscriptions: Set[WebSocket] = set() 

@app.websocket("/ws/") 
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept() 
    subscriptions.add(websocket) 
    try:
        while True:
            await websocket.receive_text() 
    except WebSocketDisconnect:
        subscriptions.remove(websocket) 

async def send_data_to_subscribers(data): 
    for websocket in subscriptions: 
        await websocket.send_json(json.dumps(data))
    
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    with engine.connect() as conn:
        for item in data:
            stmt = insert(processed_agent_data).values(
                road_state=item.road_state,
                x=item.agent_data.accelerometer.x,
                y=item.agent_data.accelerometer.y,
                z=item.agent_data.accelerometer.z,
                latitude=item.agent_data.gps.latitude,
                longitude=item.agent_data.gps.longitude,
                timestamp=item.agent_data.timestamp
            )
            conn.execute(stmt)
            conn.commit()
            await send_data_to_subscribers(item.model_dump()) 
    return {"status": "ok"}

@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    with engine.connect() as conn:
        stmt = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        result = conn.execute(stmt).first()
        return result

@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    with engine.connect() as conn:
        stmt = select(processed_agent_data) 
        result = conn.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]

@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    with engine.connect() as conn:
        stmt = update(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).values(
            road_state=data.road_state,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp
        ).returning(processed_agent_data) 
        result = conn.execute(stmt).first()
        conn.commit()
        return result

@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int):
    with engine.connect() as conn:
        stmt = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).returning(processed_agent_data) 
        result = conn.execute(stmt).first()
        conn.commit()
        return result

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)