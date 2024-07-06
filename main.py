import os
from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    port = os.environ.get("PORT", 8080)
    uvicorn.run(app, port=port, host="0.0.0.0")
