import os
from fastapi import FastAPI
import uvicorn

from db_sql import select_data

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/data")
async def data():
    return select_data("aerospace")


if __name__ == "__main__":
    port = os.environ.get("PORT", 8080)
    uvicorn.run(app, port=port, host="0.0.0.0")
