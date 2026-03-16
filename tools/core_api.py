#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

import datetime as _dt
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

import requests


class CoreApiError(Exception):
    """Core API error wrapper."""
    pass


def _parse_api_base(core_api_host: str) -> str:
    """
    Normalize the API base URL:
      - If no scheme, default to https://
      - Leave as-is if a scheme exists
    """
    parsed = urlparse(core_api_host)
    if not parsed.scheme:
        return f"https://{core_api_host}"
    return core_api_host


def _parse_iso_utc(ts: str) -> _dt.datetime:
    """
    Parse timestamps that may be ISO8601 with 'Z' or offset.
    Returns an aware datetime in UTC.
    """
    if ts.endswith("Z"):
        ts = ts.replace("Z", "+00:00")
    dt = _dt.datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        # assume UTC if naive
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc)


def _parse_possible_timestamp(ts: str) -> Optional[_dt.datetime]:
    """
    Try a few common timestamp formats and return aware UTC datetime,
    or None if parsing fails.
    """
    if not ts:
        return None
    # Try ISO first (with optional Z)
    try:
        return _parse_iso_utc(ts)
    except Exception:
        pass
    # Try "YYYY-mm-dd HH:MM:SS +0000" (as in your example)
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            dt = _dt.datetime.strptime(ts, fmt)
            return dt.astimezone(_dt.timezone.utc)
        except Exception:
            continue
    return None


def _load_token_from_file(
    token_file: str,
    token_key: str = "id_token",
    allow_expired: bool = False
) -> str:
    """
    Load a token from a JSON file. By default reads 'id_token', but will fall back to
    'access_token' or 'token' if present. Optionally check 'expires_at' if provided.

    Args:
        token_file: Path to JSON file.
        token_key: Preferred key to read (default 'id_token').
        allow_expired: If False, raise CoreApiError when the file has an 'expires_at'
                       in the past.

    Returns:
        The bearer token string.

    Raises:
        CoreApiError on file/JSON errors or missing token.
    """
    p = Path(token_file).expanduser()
    if not p.exists():
        raise CoreApiError(f"Token file not found: {p}")

    try:
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as e:
        raise CoreApiError(f"Failed to read token file {p}: {e}") from e

    # Preferred key, then common fallbacks
    candidate_keys = [token_key, "id_token", "access_token", "token"]
    tok = None
    for k in candidate_keys:
        if k in data and isinstance(data[k], str) and data[k].strip():
            tok = data[k].strip()
            break

    if not tok:
        raise CoreApiError(
            f"Token not found in file {p}. Tried keys: {', '.join(candidate_keys)}"
        )

    # Optional expiry check if file provides expires_at
    expires_at = data.get("expires_at")
    if expires_at:
        exp = _parse_possible_timestamp(expires_at)
        if exp is None:
            logging.warning(
                "Token file %s has 'expires_at' but it couldn't be parsed: %r",
                str(p), expires_at
            )
        else:
            now = _dt.datetime.now(tz=_dt.timezone.utc)
            if exp <= now and not allow_expired:
                raise CoreApiError(
                    f"Token from {p} appears expired at {exp.isoformat()} UTC."
                )

    return tok


class CoreApi:
    """
    Interface to the FABRIC Core API.

    You can pass either:
      - token=<bearer string>
      - or token_file=<path to JSON> (containing 'id_token' by default)

    If both are provided, 'token' takes precedence.
    """
    def __init__(
        self,
        core_api_host: str,
        token: Optional[str] = None,
        *,
        token_file: Optional[str] = None,
        token_key: str = "id_token",
        allow_expired_token_file: bool = False,
        timeout: float = 15.0,
        session: Optional[requests.Session] = None
    ):
        """
        Args:
            core_api_host: Host or full base URL for Core API.
            token: Bearer token (if provided, this is used directly).
            token_file: Path to JSON with an 'id_token' (default) or other key.
            token_key: Which key to read from token_file (default 'id_token').
            allow_expired_token_file: If True, do not fail when token_file's 'expires_at'
                                      is in the past (if present).
            timeout: Per-request timeout (seconds).
            session: Optional requests.Session for connection pooling.
        """
        self.api_server = _parse_api_base(core_api_host)
        self.timeout = timeout
        self.session = session or requests.Session()

        if token is None:
            if token_file:
                token = _load_token_from_file(
                    token_file=token_file,
                    token_key=token_key,
                    allow_expired=allow_expired_token_file
                )
            else:
                raise CoreApiError("You must provide either 'token' or 'token_file'.")

        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ------------- Low-level helpers -------------

    def _request(self, method: str, path: str, *, params: Dict[str, Any] = None, json_body: Any = None) -> requests.Response:
        """
        Perform an HTTP request with consistent error handling and timeouts.
        """
        url = f"{self.api_server}{path if path.startswith('/') else '/' + path}"
        try:
            resp = self.session.request(
                method=method.upper(),
                url=url,
                headers=self.headers,
                params=params,
                json=json_body,
                timeout=self.timeout,
            )
            self.raise_for_status(response=resp)
            return resp
        except requests.RequestException as e:
            raise CoreApiError(f"Request to {url} failed: {e}") from e

    @staticmethod
    def raise_for_status(response: requests.Response):
        """
        Checks the response status and raises CoreApiError if the request was unsuccessful.
        """
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            try:
                message = response.json()
            except json.JSONDecodeError:
                # Keep raw text as a fallback for diagnostics
                message = {"message": response.text or "Unknown error occurred while processing the request."}
            raise CoreApiError(f"Error {response.status_code}: {e}. Message: {message}")

    # ------------- Projects (generic UIS wrapper) -------------

    @staticmethod
    def _bool_str(v: Optional[bool]) -> Optional[str]:
        if v is None:
            return None
        return "true" if v else "false"

    def projects_get(
        self,
        *,
        # Common filters seen in UIS/Portal
        search: Optional[str] = None,
        person_uuid: Optional[str] = None,
        status: Optional[str] = None,        # e.g., "active" | "expired"
        exact_match: Optional[bool] = None,
        as_self: Optional[bool] = None,
        # Paging & sorting
        offset: int = 0,
        limit: int = 50,
        sort_by: Optional[str] = None,       # e.g., "name" | "created_on"
        order_by: Optional[str] = None,      # "asc" | "desc"
        # Pass-through for any future/unknown parameters
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Call UIS/Core 'GET /projects' with flexible filters.

        Returns the full payload (commonly includes 'results', 'size', 'total').

        Parameters map to typical UIS query args; any not provided are omitted.
        Booleans are serialized as 'true'/'false'. 'tags' (list) becomes CSV.

        Examples:
            core.projects_get(person_uuid=uid, limit=100, sort_by="name", order_by="asc")
            core.projects_get(search="FABRIC", exact_match=True)
            core.projects_get(status="active", role="owner", include_memberships=True)

        Tip: Use iter_projects(...) below to auto-paginate.
        """
        params: Dict[str, Any] = {
            "offset": max(0, int(offset)),
            "limit": max(1, int(limit)),
        }

        if search:
            params["search"] = search
        if person_uuid:
            params["person_uuid"] = person_uuid
        if status:
            params["status"] = status
        if sort_by:
            params["sort_by"] = sort_by
        if order_by:
            params["order_by"] = order_by
        if exact_match is not None:
            params["exact_match"] = self._bool_str(exact_match)
        if as_self is not None:
            params["as_self"] = self._bool_str(as_self)

        if extra:
            # Allow arbitrary future params to be passed through
            for k, v in extra.items():
                if isinstance(v, bool):
                    params[k] = self._bool_str(v)
                else:
                    params[k] = v

        resp = self._request("GET", "/projects", params=params)
        return resp.json()

    def iter_projects(
        self,
        **kwargs: Any,
    ):
        """
        Generator over all projects matching filters. Yields each project dict.

        Uses 'projects_get' and follows 'offset/limit' until all 'total' are read.
        Accepts the same kwargs as projects_get (search, person_uuid, etc.).
        """
        # Start with caller-provided offset/limit (default 0/50)
        offset = int(kwargs.pop("offset", 0) or 0)
        limit = int(kwargs.pop("limit", 50) or 50)

        while True:
            page = self.projects_get(offset=offset, limit=limit, **kwargs)
            results = page.get("results") or []
            for r in results:
                yield r

            size = page.get("size") or len(results)
            total = page.get("total")
            offset += size

            # Stop if we reached end or no progress
            if size == 0 or (total is not None and offset >= total):
                break

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """
        Convenience wrapper for GET /projects/{uuid}.
        Returns the first item in 'results' or raises if missing.
        """
        if not project_id:
            raise CoreApiError("project_id must be provided.")

        resp = self._request("GET", f"/projects/{project_id}")
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            raise CoreApiError(f"No project found for id: {project_id}")
        return results[0]

    # ------------- Projects: paginate & collect -------------

    def collect_projects(
        self,
        *,
        active: Optional[bool] = None,
        # common filters you may still want to use server-side
        search: Optional[str] = None,
        person_uuid: Optional[str] = None,
        exact_match: Optional[bool] = None,
        sort_by: Optional[str] = None,
        order_by: Optional[str] = None,
        page_limit: int = 200,
    ) -> List[dict]:
        """
        Fetch ALL pages of /projects (within the caller's normal scope) and
        optionally filter by active status client-side.

        Args:
            active:
                - True  => only active projects
                - False => only inactive/expired projects
                - None  => no client-side active filter (return all)
            search, person_uuid, role, exact_match, include_memberships, tags:
                Passed to the server per page (only filters supported by backend are applied server-side).
            sort_by, order_by:
                Sorting applied server-side per page; we still iterate through all pages.
            page_limit:
                Page size used for pagination (default 200).

        Returns:
            List[dict]: aggregated projects from all pages, with optional client-side active filtering.
        """
        offset = 0
        limit = max(1, int(page_limit))
        all_projects: List[dict] = []

        while True:
            page = self.projects_get(
                offset=offset,
                limit=limit,
                search=search,
                person_uuid=person_uuid,
                exact_match=exact_match,
                sort_by=sort_by,
                order_by=order_by,
            )
            results = page.get("results") or []
            all_projects.extend(results)

            size = page.get("size") or len(results)
            total = page.get("total")
            offset += size

            if size == 0 or (total is not None and offset >= total):
                break

        if active is None:
            return all_projects

        # Client-side active filtering
        if active:
            return [p for p in all_projects if p.get("active")]
        else:
            return all_projects


    # ------------- People (generic UIS wrapper) -------------
    def people_get(
        self,
        *,
        search: Optional[str] = None,          # name/email/uuid search
        exact_match: Optional[bool] = None,    # "true"/"false"
        offset: int = 0,
        limit: int = 50,
        sort_by: Optional[str] = None,         # e.g., "name" | "email"
        order_by: Optional[str] = None,        # "asc" | "desc"
        # Future/pass-through
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Call UIS/Core 'GET /people' with flexible filters.
        Returns the full JSON payload (commonly includes 'results', 'size', 'total').
        """
        params: Dict[str, Any] = {
            "offset": max(0, int(offset)),
            "limit": max(1, int(limit)),
        }

        if search:
            params["search"] = search
        if exact_match is not None:
            params["exact_match"] = self._bool_str(exact_match)
        if sort_by:
            params["sort_by"] = sort_by
        if order_by:
            params["order_by"] = order_by

        if extra:
            for k, v in extra.items():
                params[k] = self._bool_str(v) if isinstance(v, bool) else v

        resp = self._request("GET", "/people", params=params)
        return resp.json()

    def iter_people(self, **kwargs: Any):
        """
        Generator over all 'people' matching filters. Yields each person dict.
        Accepts the same kwargs as people_get (search, exact_match, status, etc.).
        """
        offset = int(kwargs.pop("offset", 0) or 0)
        limit = int(kwargs.pop("limit", 50) or 50)

        while True:
            page = self.people_get(offset=offset, limit=limit, **kwargs)
            results = page.get("results") or []
            for r in results:
                yield r

            size = page.get("size") or len(results)
            total = page.get("total")
            offset += size

            if size == 0 or (total is not None and offset >= total):
                break

    @staticmethod
    def _is_active(person: dict) -> bool:
        """
        Determine whether a person record indicates an active user.

        The list endpoint only returns (uuid, name, email), so 'active' may
        not be present.  For detail records the check order is:

        1. ``active`` field is truthy  → active
        2. ``roles`` list contains a role named ``fabric-active-users`` → active
        3. Otherwise → not active
        """
        if person.get("active"):
            return True
        for role in person.get("roles") or []:
            if isinstance(role, dict) and role.get("name") == "fabric-active-users":
                return True
            if isinstance(role, str) and role == "fabric-active-users":
                return True
        return False

    def collect_people(
        self,
        *,
        active: Optional[bool] = None,
        # server-side passthrough
        search: Optional[str] = None,
        exact_match: Optional[bool] = None,
        sort_by: Optional[str] = None,
        order_by: Optional[str] = None,
        page_limit: int = 200,
        extra: Optional[Dict[str, Any]] = None,
    ) -> List[dict]:
        """
        Fetch ALL pages of /people and return a list, with optional client-side filters.

        Args:
            active: if True returns only active; if False only inactive; if None no filter.
                    Active is determined by the ``active`` field or by having the
                    ``fabric-active-users`` role.
        """
        people: List[dict] = []
        for person in self.iter_people(
            search=search,
            exact_match=exact_match,
            sort_by=sort_by,
            order_by=order_by,
            limit=page_limit,
            extra=extra,
        ):
            people.append(person)

        if active is None:
            return people
        if active:
            return [p for p in people if self._is_active(p)]
        else:
            return [p for p in people if not self._is_active(p)]

    def get_person(self, person_uuid: str) -> Dict[str, Any]:
        """
        Convenience wrapper for GET /people/{uuid}.
        Returns the first 'results' item or raises if none.
        """
        if not person_uuid:
            raise CoreApiError("person_uuid must be provided.")
        resp = self._request("GET", f"/people/{person_uuid}")
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            raise CoreApiError(f"No person found for id: {person_uuid}")
        return results[0]

    def get_person_details(self, person_uuid: str) -> Dict[str, Any]:
        """
        Fetch enriched person record from ``/core-api-metrics/people-details/{uuid}``.

        This endpoint returns ``active`` (bool), ``bastion_login``, ``last_updated``,
        ``roles``, and other fields not available on the basic ``/people/{uuid}`` endpoint.
        """
        if not person_uuid:
            raise CoreApiError("person_uuid must be provided.")
        resp = self._request("GET", f"/core-api-metrics/people-details/{person_uuid}")
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            raise CoreApiError(f"No person details found for id: {person_uuid}")
        return results[0]

if __name__ == "__main__":
    # Example usage with a token file:
    token_path = "/Users/kthare10/work/id_token_prod.json"

    core_api = CoreApi(
        core_api_host="https://uis.fabric-testbed.net",
        token_file=token_path,        # <- supply token_file OR token="..."
    )

    projects = core_api.collect_projects(active=True)
    print(len(projects))
    users = core_api.collect_people()
    print(len(users))