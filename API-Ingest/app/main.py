from fastapi import FastAPI, status, HTTPException

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


import json

from pydantic import BaseModel


from datetime import datetime
from kafka import KafkaProducer, producer


class Jobs(BaseModel):
    JobName : str
    Company : str
    JD: str
    DatePosted : str
    YOE : str
    Location : str
    Website : str
    JobFunction : str
    Industry : str
    Specialization : str
    Qualification : str
    HiringLocation : str
    Role : str
    Vacancies : int


app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/jobinformation")
async def post_job_information(job_item: Jobs):
    print("Message Received")
    try:

        json_of_job_item = jsonable_encoder(job_item)

        json_as_string = json.dumps(json_of_job_item)
        print(json_as_string)

        produce_kafka(json_as_string)

        return JSONResponse(content=json_of_job_item, status_code=201)

    except ValueError:
        return JSONResponse(content=jsonable_encoder(job_item), status_code=400)



def produce_kafka(json_as_string):
    producer = KafkaProducer(bootstrap_servers='localhost:9093',acks=1)

    producer.send('ingestion-topic',bytes(json_as_string,'utf-8'))
    producer.flush()

