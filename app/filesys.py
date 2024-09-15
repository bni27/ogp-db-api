from os import environ
from pathlib import Path
from typing import Generator

BUCKET_MOUNT = Path(environ.get("BUCKET_MOUNT", "/data"))


def build_verified_path(verified: bool = False) -> Path:
    verification_folder = Path("verified") if verified else Path("unverified")
    return BUCKET_MOUNT / verification_folder


def build_asset_path(
    asset_class: str,
    verified: bool = False,
    create: bool = False,
    raise_if_absent: bool = True,
) -> Path:
    asset_class_path = build_verified_path(verified) / Path(asset_class)
    if create:
        asset_class_path.mkdir()
    if raise_if_absent and not asset_class_path.exists():
        raise ValueError
    return asset_class_path


def build_raw_file_path(
    file_name: str,
    asset_class: str,
    verified: bool = False,
) -> Path:
    return build_asset_path(asset_class, verified) / Path(file_name)


def _get_files(
    asset_class: str,
    verified: bool = False,
) -> Generator[Path, None, None]:
    return (f for f in build_asset_path(asset_class, verified).iterdir() if f.is_file())


def get_data_files(
    asset_class: str, verified: bool = False, extension: str = ".csv"
) -> Generator[Path, None, None]:
    return (f for f in _get_files(asset_class, verified) if f.name.endswith(extension))


def get_directories(
    verified: bool = False,
) -> Generator[Path, None, None]:
    return (d for d in build_verified_path(verified).iterdir() if d.is_dir())
