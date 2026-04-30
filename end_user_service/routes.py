from service_manager import ServiceManager
from logs_producer import LogsProducer
from fastapi import APIRouter
import os


prod_config = {"bootstrap.servers": os.getenv("KAFKA_URI", "localhost:9092")}
logger = LogsProducer(prod_config)



es_loader = ServiceManager(logger)
router = APIRouter()


@router.get("/suspects/searching/")
def search_suspect(query:str):
    return es_loader.es.search(query=query)