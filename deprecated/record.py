
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, status, UploadFile
from fastapi.responses import FileResponse

from app.auth import AuthLevel, User, validate_api_key
from app.db import (
    delete_raw_table,
    delete_stage_table,
    load_raw_data,
    stage_data,
    union_prod,
)
from app.filesys import (
    build_asset_path,
    build_raw_file_path,
    get_data_files,
    get_directories,
)
from app.pg import Record, row_count, select_data
from app.pg import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.sql import prod_table, stage_schema
from app.db import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)


@router.put("/assetClasses/{asset_class}/files/{file_name}/updateRecord")
def update_record(
    record: Record,
    asset_class: str,
    file_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    project_id = record.project_id
    sample = record.sample
    data = record.data
    data.update({"sample": sample, "project_id": project_id})
    file_path = build_raw_file_path(file_name, asset_class, verified)
    p_id_idx: int | None = None
    samp_idx: int | None = None
    with open(file_path, "r", encoding="utf-8-sig") as f:
        headers = [h.lower() for h in f.readline().split(",")]
        print(headers)
        for i, h in enumerate(headers):
            if h == "project_id":
                p_id_idx = i
            if h == "sample":
                samp_idx = i
        if any([p_id_idx is None, samp_idx is None]):
            print("Couldn't find project_id or sample columns.")
            return "couldn't find ID columns"
        if not all(k.lower() in headers for k in data.keys()):
            print("Extra headers specified")
            return "Extra headers specified"
        temp_file = file_path.parent / Path(f"{file_path.stem}.tmp")
        not_yet_replaced: bool = True
        with open(temp_file, "w") as t:
            t.write(",".join(headers))
            for l in f.readlines():
                row = l.split(",")
                if (row[p_id_idx] == project_id) and (row[samp_idx] == sample):
                    t.write(
                        ",".join([data.get(h, row[i]) for i, h in enumerate(headers)])
                    )
                    not_yet_replaced = False
                else:
                    t.write(l)
            if not_yet_replaced:
                t.write(",".join([data.get(h, "") for h in headers]))
    try:
        table = load_raw_data(temp_file)
        temp_file.replace(file_path)
    except Exception as e:
        logger.exception(e)
        raise e
    return {
        "table_name": table,
        "rows": row_count(table)[0],
    }