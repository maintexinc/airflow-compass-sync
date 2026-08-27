"""
Secret Manager access with a simple in-process cache.
Secrets are re-fetched at most once per hour so credential rotation
takes effect without restarting Airflow.
"""

import json
import time

from google.cloud import secretmanager

_cache: dict[str, tuple[str, float]] = {}
_TTL = 3600  # seconds


def get_secret(secret_id: str, project: str) -> str:
    now = time.time()
    if secret_id in _cache:
        value, expiry = _cache[secret_id]
        if now < expiry:
            return value

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    value = response.payload.data.decode("utf-8")
    _cache[secret_id] = (value, now + _TTL)
    return value


def get_ionapi(project: str, env: str = "prd") -> dict:
    """Return parsed .ionapi JSON for the given environment (prd or trn)."""
    return json.loads(get_secret(f"compass-ionapi-{env}", project))


def get_cloudsql_dsn(project: str, env: str = "prd") -> str:
    return get_secret(f"cloudsql-dsn-{env}", project)
