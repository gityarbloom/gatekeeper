# from elastic_loader import play_program
from fastapi import FastAPI
from routes import router
import uvicorn


app = FastAPI()
app.include_router(router=router)

if __name__ == "__main__":
    uvicorn.run(app=app)