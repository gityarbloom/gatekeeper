from elastic_loader import ElasticLoader
from fastapi import APIRouter



es_loader = ElasticLoader()
router = APIRouter()


@router.get("/suspects/searching/")
def search_suspect(query:str):
    return es_loader.es.search(query=query)