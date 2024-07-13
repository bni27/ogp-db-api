import json
import os
from time import time

from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey.RSA import import_key, RsaKey
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

from app.user import AuthLevel, User

api_key_header = APIKeyHeader(name="X-API-Key")


rsa_key: RsaKey = import_key(
    os.environ.get("RSA_KEY").encode().decode("unicode_escape")
)
ENCRYPTER = PKCS1_OAEP.new(rsa_key.public_key())
DECRYPTER = PKCS1_OAEP.new(rsa_key)


def decode_api_key(api_key: str) -> dict[str, str | int]:
    bytes_key = bytes.fromhex(api_key)
    _details = DECRYPTER.decrypt(bytes_key)
    return json.loads(_details)


def generate_api_key(name: str, auth_level: AuthLevel, exp_date: int) -> str:
    user = User.model_validate(
        {
            "name": name,
            "auth_level": auth_level,
            "exp_date": exp_date,
        }
    )
    _details = user.model_dump_json().encode()
    return ENCRYPTER.encrypt(_details).hex()


def validate_api_key(api_key_header: str = Security(api_key_header)) -> User:
    try:
        api_key_details = decode_api_key(api_key_header)
        user = User.model_validate(api_key_details)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
        )
    if user.exp_date < time():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API key expired. Contact your administrator for access.",
        )
    return user
