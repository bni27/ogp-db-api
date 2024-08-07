import os
from pathlib import Path

from fastapi import Depends, FastAPI, status
from fastapi.responses import FileResponse, JSONResponse
from psycopg2 import OperationalError
import uvicorn

from app.auth import validate_api_key
from app.pg import get_cursor
from app.routers import auth, data

app = FastAPI(
    title="Oxford Global Projects MegaProjects Data",
    version="0.2.0",
    contact={
        "name": "Ian Bakst",
        "email": "ian.bakst@oxfordglobalprojects.com",
    },
)

app.include_router(
    auth.router, prefix="/api/v1/auth", dependencies=[Depends(validate_api_key)]
)

app.include_router(
    data.router, prefix="/api/v1/data", dependencies=[Depends(validate_api_key)]
)

@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    favicon_path = Path(__file__).parent / Path("app") / Path("favicon.jpg")
    return FileResponse(favicon_path)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    db_ok = True
    filesys_ok = True
    try:
        with get_cursor():
            pass
        assert os.path.exists("/data")
    except OperationalError as e:
        print(e)
        db_ok = False
    except AssertionError:
        filesys_ok = False
    is_ok = all([db_ok, filesys_ok])
    health_status = status.HTTP_200_OK if is_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=health_status,
        content={
            "healthy": is_ok,
            "db_connection": db_ok,
            "file_system_connection": filesys_ok,
        },
    )


if __name__ == "__main__":
    uvicorn.run(app, port=int(os.environ.get("PORT", 8080)), host="0.0.0.0")
