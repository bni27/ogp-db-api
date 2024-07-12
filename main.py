import os

from fastapi import Depends, FastAPI, status
from fastapi.responses import JSONResponse
import uvicorn

from app.auth import validate_api_key
from app.db import connector
from app.routers import auth, data

app = FastAPI()

app.include_router(
    auth.router,
    prefix="/api/v1/auth",
    dependencies=[Depends(validate_api_key)]
)

app.include_router(
    data.router,
    prefix="/api/v1/data",
    dependencies=[Depends(validate_api_key)]
)


@app.get("/health", status_code=status.HTTP_204_NO_CONTENT)
async def root():
    try:
        c = connector().connect()
        c.close()
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"health": "Cannot connect to DB"}
        )
    return


if __name__ == "__main__":
    uvicorn.run(app, port=os.environ.get("PORT", 8080), host="0.0.0.0")
