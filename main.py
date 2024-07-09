import os
from fastapi import FastAPI
import uvicorn

from db_sql import select_data, union_prod

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/data")
async def data(asset_class: str | None = None):
    return select_data(asset_class)


@app.post("/update")
async def update():
    # load new raw datasets
    # process them to stage
    # union them to prod table
    return union_prod()


if __name__ == "__main__":
    port = os.environ.get("PORT", 8080)
    uvicorn.run(app, port=port, host="0.0.0.0")
