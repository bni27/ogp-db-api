import os
from pathlib import Path

from fastapi import Depends, FastAPI, status
from fastapi.responses import FileResponse, JSONResponse
from psycopg2 import OperationalError

from app.auth import validate_api_key
from app.table import DB_MGMT
from app.routers import auth, data


app = FastAPI(
    title="Oxford Global Projects MegaProjects Data",
    version="0.2.0",
    contact={
        "name": "Ian Bakst",
        "email": "ian.bakst@oxfordglobalprojects.com",
    },
    root_path="/api/v1",
)

app.include_router(
    auth.router, prefix="/auth", dependencies=[Depends(validate_api_key)]
)

app.include_router(
    data.router, prefix="/data", dependencies=[Depends(validate_api_key)]
)


@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    favicon_path = Path(__file__).parent / Path("app") / Path("favicon.jpg")
    return FileResponse(favicon_path)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health(db: DB_MGMT):
    db_ok = True
    filesys_ok = True
    try:
        with db.get_session().connection():
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
