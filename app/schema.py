def _verified_name(verified: bool = False) -> str:
    return "verified" if verified else "unverified"


def raw_schema(verified: bool = False) -> str:
    return f"raw_{_verified_name(verified)}"


def stage_schema(verified: bool = False) -> str:
    return f"stage_{_verified_name(verified)}"


def prod_table(verified: bool = False) -> str:
    return f"{_verified_name(verified)}_projects"
