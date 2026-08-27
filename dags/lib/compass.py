"""
Compass REST API client for Infor Data Lake.

Auth flow: 2-legged OAuth using service account credentials from .ionapi config.
Token endpoint: {pu}{ot}
Base URL:        {iu}/{ti}/DATAFABRIC/compass/v2

V2 API flow:
  POST /jobs/                        -> submit query, returns queryId (HTTP 202)
  GET  /jobs/{queryId}/status/       -> poll; HTTP 201 = FINISHED, 202 = RUNNING
  GET  /jobs/{queryId}/result/       -> paginate with ?offset=&limit=
  PUT  /jobs/{queryId}/cancel/       -> cancel a running job
"""

import json
import logging
import time
from collections.abc import Iterator

import requests

log = logging.getLogger(__name__)


def _extract_error_detail(body) -> str | None:
    """Pull a human-readable error out of a Compass FAILED/CANCELED status body.

    Compass reports failures as a reason code + message -- e.g. code 401
    "Invalid property name references: '<obj.prop>' ... or you do not have
    appropriate permissions" (see the Data Lake query error-handling tables in
    docs/inforos_2026.x_datafabrug__en-us.pdf). The status JSON's field names
    are not documented and vary across API versions, so probe the known shapes,
    then fall back to the raw body so the detail is never silently dropped.
    """
    if not isinstance(body, dict):
        return None
    for key in ("message", "errorMessage", "error", "statusMessage", "reason"):
        val = body.get(key)
        if isinstance(val, str) and val.strip():
            return val
    # Some versions return a list of {code, message} objects under `messages`.
    messages = body.get("messages")
    if isinstance(messages, list):
        parts = []
        for m in messages:
            if isinstance(m, dict):
                code = m.get("code") or m.get("messageCode")
                text = m.get("message") or m.get("description")
                if text:
                    parts.append(f"{code}: {text}" if code else str(text))
            elif isinstance(m, str) and m.strip():
                parts.append(m)
        if parts:
            return "; ".join(parts)
    return None


class CompassClient:
    def __init__(self, ionapi: dict):
        self._ionapi = ionapi
        self._token = None
        self._token_expiry = 0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 30:
            return self._token

        c = self._ionapi
        url = c["pu"] + c["ot"]
        resp = requests.post(url, data={
            "grant_type": "password",
            "username": c["saak"],
            "password": c["sask"],
            "client_id": c["ci"],
            "client_secret": c["cs"],
        }, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        self._token = body["access_token"]
        self._token_expiry = time.time() + body.get("expires_in", 3600)
        return self._token

    def _auth_header(self) -> dict:
        """Authorization header only — for GET requests with no body."""
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _headers(self) -> dict:
        """Auth + Content-Type for POST requests with a plain-text body."""
        return {**self._auth_header(), "Content-Type": "text/plain"}

    # ------------------------------------------------------------------
    # Compass async query: submit -> poll -> pages
    # ------------------------------------------------------------------

    def _base_url(self) -> str:
        tenant = self._ionapi["ti"]
        iu = self._ionapi["iu"].rstrip("/")
        return f"{iu}/{tenant}/DATAFABRIC/compass/v2"

    def query_pages(self, sql: str, timeout: int = 3300) -> Iterator[list[dict]]:
        """Submit a SQL query and yield result rows one page at a time.

        Streaming keeps peak memory bounded by the page size (10k rows)
        rather than the full result set.
        """
        base = self._base_url()

        # 1. Submit
        resp = requests.post(
            f"{base}/jobs/",
            headers=self._headers(),
            data=sql,
            timeout=30,
        )
        resp.raise_for_status()
        query_id = resp.json()["queryId"]

        # 2. Poll until finished using long-polling (up to 25s server wait per call)
        deadline = time.time() + timeout
        while True:
            if time.time() > deadline:
                requests.put(f"{base}/jobs/{query_id}/cancel/",
                             headers=self._auth_header(), timeout=30)
                raise TimeoutError(f"Compass query {query_id} timed out after {timeout}s")

            status_resp = requests.get(
                f"{base}/jobs/{query_id}/status/",
                headers=self._auth_header(),
                params={"timeout": 25},
                timeout=35,
            )
            status_resp.raise_for_status()

            # HTTP 201 = FINISHED, HTTP 202 = still RUNNING
            if status_resp.status_code == 201:
                body = status_resp.json()
                status = body.get("status")
                if status == "FINISHED":
                    break
                if status in ("FAILED", "CANCELED"):
                    detail = _extract_error_detail(body)
                    # Always log the failing SQL and full response so a future
                    # failure is diagnosable from the task log alone; the raw
                    # body is the backstop when no known detail field matches.
                    log.error(
                        "Compass query %s ended %s\nSQL: %s\nResponse: %s",
                        query_id, status, sql, json.dumps(body, default=str)[:4000])
                    if not detail:
                        detail = json.dumps(body, default=str)[:1000]
                    msg = f"Compass query {query_id} ended with status {status!r}"
                    if detail:
                        msg += f": {detail}"
                    raise RuntimeError(msg)

        # 3. Fetch pages using limit/offset, yielding each as it arrives
        offset = 0
        limit = 10000
        while True:
            page_resp = requests.get(
                f"{base}/jobs/{query_id}/result/",
                headers={**self._auth_header(), "Accept": "application/json"},
                params={"offset": offset, "limit": limit},
                timeout=60,
            )
            page_resp.raise_for_status()
            page_rows = page_resp.json()
            if page_rows:
                yield page_rows
            if len(page_rows) < limit:
                break
            offset += limit

    def query(self, sql: str, timeout: int = 3300) -> list[dict]:
        """Submit a SQL query and return all rows as a list of dicts.

        Only for queries with small results (e.g. aggregates); use
        query_pages() for table extracts.
        """
        rows = []
        for page in self.query_pages(sql, timeout=timeout):
            rows.extend(page)
        return rows

    def get_max_lastmodified(self, table: str) -> str | None:
        """Return the max lastmodified timestamp for a table, or None if empty."""
        sql = f"SELECT max(infor.lastmodified()) as ts FROM infor.includedeleted('{table}')"
        rows = self.query(sql)
        if rows:
            return rows[0].get("ts")
        return None

    def fetch_incremental(self, table: str, since: str, until: str) -> Iterator[list[dict]]:
        """Yield pages of rows modified in [since, until] inclusive."""
        sql = f"""
            SELECT infor.lastmodified() as xxx_last_modified_datetime,
                   CURRENT_TIMESTAMP    as xxx_extraction_datetime,
                   *
              FROM infor.includedeleted('{table}')
             WHERE infor.lastmodified() >= '{since}'
               AND infor.lastmodified() <= '{until}'
        """
        return self.query_pages(sql)

    def fetch_initial(self, table: str, until: str) -> Iterator[list[dict]]:
        """Yield pages of all rows up to `until` for first-ever load."""
        sql = f"""
            SELECT infor.lastmodified() as xxx_last_modified_datetime,
                   CURRENT_TIMESTAMP    as xxx_extraction_datetime,
                   *
              FROM infor.includedeleted('{table}')
             WHERE infor.lastmodified() <= '{until}'
        """
        return self.query_pages(sql)
