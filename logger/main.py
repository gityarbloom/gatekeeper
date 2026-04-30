from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from consumer import LoggerConsumer
from mongo import MongoLoader
from threading import Thread
import uvicorn
import time
import os




cons_config = {
    'bootstrap.servers': os.getenv("KAFKA_URI"),
    'group.id': 'gatekeeper',
    'auto.offset.reset': 'earliest'#,
        }
mongo_config = os.getenv("MONGO_URI")
mongo_db = "gatekeeper_db"
mongo_coll = "logs_coll"



def run(mongo):
    consumer = LoggerConsumer(cons_config)
    batch = []
    last_flush_time = time.time()
    for log in consumer.consum_logs():
        batch.append(log)
        now = time.time()
        if len(batch) >= 10:
            mongo.insert(mongo_db, mongo_coll, batch)
            batch.clear()
            last_flush_time = now
        elif (now - last_flush_time) >= 30 and batch:
            mongo.insert(mongo_db, mongo_coll, batch)
            batch.clear()
            last_flush_time = now




@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mongo = MongoLoader(mongo_config)
    thread = Thread(target=run, args=(app.state.mongo,),daemon=True)
    thread.start()
    print("Consumer thread started")
    yield
    print("Shutting down...")


app = FastAPI(lifespan=lifespan)


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/last_logs")
def last_logs(request: Request, limit:int =10):
    mongo = request.app.state.mongo
    for d in docs:
        d["_id"] = str(d["_id"])
    docs = list(mongo.get_last_docs(mongo_db, mongo_coll, limit))
    return docs


@app.get("/logs_by_service")
def logs_by_service(request:Request, service_name:str):
    mongo = request.app.state.mongo
    docs = mongo.get_docs_by_param(mongo_db, mongo_coll, service_name)
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs




if __name__ == "__main__":
    uvicorn.run("main:app", "--host", "0.0.0.0", "--port", "9990")