#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

import datetime as _dt
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
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


class CoreApi:
    """
    Interface to the FABRIC Core API.
    """
    def __init__(self, core_api_host: str, token: str, *, timeout: float = 15.0, session: Optional[requests.Session] = None):
        """
        Args:
            core_api_host: Host or full base URL for Core API.
            token: Bearer token.
            timeout: Per-request timeout (seconds).
            session: Optional requests.Session for connection pooling.
        """
        self.api_server = _parse_api_base(core_api_host)
        self.timeout = timeout
        self.session = session or requests.Session()
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ------------- Low-level helpers -------------

    def _req(self, method: str, path: str, *, params: Dict[str, Any] = None, json_body: Any = None) -> requests.Response:
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

    # ------------- People -------------

    def get_user_id(self) -> str:
        """
        Return caller's user UUID via /whoami.
        """
        resp = self._req("GET", "/whoami")
        logging.debug(f"GET WHOAMI Response : {resp.json()}")
        results = resp.json().get("results") or []
        if not results or not results[0].get("uuid"):
            raise CoreApiError("Malformed /whoami response: missing results/uuid.")
        return results[0]["uuid"]

    def get_user_info_by_email(self, *, email: str) -> Optional[dict]:
        """
        Look up user by email. Returns the first exact match or None.
        """
        if not email:
            raise CoreApiError("Email must be specified.")
        params = {
            "search": email,
            "exact_match": "true",
            "offset": 0,
            "limit": 5,
        }
        resp = self._req("GET", "/people", params=params)
        logging.debug(f"GET PEOPLE Response : {resp.json()}")
        results = resp.json().get("results") or []
        return results[0] if results else None

    def get_user_info(self, *, uuid: Optional[str] = None, email: Optional[str] = None) -> dict:
        """
        Return user object either by UUID, by email, or for the caller if neither is given.
        """
        if email is not None:
            info = self.get_user_info_by_email(email=email)
            if info is None:
                raise CoreApiError(f"No user found with email: {email}")
            return info

        if uuid is None:
            uuid = self.get_user_id()

        resp = self._req("GET", f"/people/{uuid}", params={"as_self": "true"})
        logging.debug(f"GET PEOPLE/{uuid} Response : {resp.json()}")
        results = resp.json().get("results") or []
        if not results:
            raise CoreApiError(f"No user found with uuid: {uuid}")
        return results[0]

    # ------------- Projects -------------

    def __get_user_project_by_id(self, *, project_id: str) -> List[dict]:
        """
        Return project by ID. API typically returns a list under 'results'.
        """
        resp = self._req("GET", f"/projects/{project_id}")
        logging.debug(f"GET Project/{project_id} Response : {resp.json()}")
        return resp.json().get("results") or []

    def __get_user_projects(self, *, project_name: Optional[str] = None, uuid: Optional[str] = None) -> List[dict]:
        """
        Return user's projects (optionally filtered by project_name).
        Handles pagination robustly.
        """
        if uuid is None:
            uuid = self.get_user_id()

        result: List[dict] = []
        offset = 0
        limit = 50

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "person_uuid": uuid,
                "sort_by": "name",
                "order_by": "asc",
            }
            if project_name:
                params["search"] = project_name

            resp = self._req("GET", "/projects", params=params)
            payload = resp.json()
            logging.debug(f"GET Projects Response (offset={offset}, limit={limit}): {payload}")

            size = payload.get("size") or 0
            total = payload.get("total") or 0
            projects = payload.get("results") or []

            result.extend(projects)

            offset += size  # <-- FIX: advance by size
            if offset >= total or size == 0:
                break

        return result

    def get_user_projects(self, project_name: str = "all", project_id: str = "all", uuid: str = None) -> List[dict]:
        """
        Get user's projects:
          - specific project_id
          - by project_name
          - or all (default)

        Filters out expired projects (unless a specific project is requested, in which case it raises).
        Ensures the user has some membership (member/creator/owner) in returned projects.
        """
        specific_id = project_id is not None and project_id != "all"
        specific_name = project_name is not None and project_name != "all"

        if specific_id:
            projects = self.__get_user_project_by_id(project_id=project_id)
        elif specific_name:
            projects = self.__get_user_projects(project_name=project_name, uuid=uuid)
        else:
            projects = self.__get_user_projects(uuid=uuid)

        ret_val: List[dict] = []
        now = _dt.datetime.now(tz=_dt.timezone.utc)

        for p in projects:
            # Expiration check
            expires_on = p.get("expires_on")
            if expires_on:
                try:
                    expires_dt = _parse_iso_utc(expires_on)
                except Exception:
                    # If we cannot parse, be conservative and keep it
                    expires_dt = None

                if expires_dt and now > expires_dt:
                    if not specific_id and not specific_name:
                        # Skip expired in "all" results
                        continue
                    else:
                        # Explicit request for an expired project → error
                        raise CoreApiError(f"Project {p.get('name')} is expired!")

            memberships = p.get("memberships") or {}
            is_member = bool(
                memberships.get("is_member") or memberships.get("is_creator") or memberships.get("is_owner")
            )

            if not is_member:
                # If user has no effective membership, error for specific requests; skip for "all"
                if specific_id or specific_name:
                    raise CoreApiError(f"User is not a member of Project: {p.get('uuid')}")
                else:
                    continue

            project: Dict[str, Any] = {
                "name": p.get("name"),
                "uuid": p.get("uuid"),
            }

            # Include tags & memberships only for a specific project-id request (matches your original logic intent)
            if specific_id:
                project["tags"] = p.get("tags")
                project["memberships"] = memberships

            ret_val.append(project)

        if len(ret_val) == 0:
            raise CoreApiError(f"User is not a member of Project: {project_id}:{project_name}")

        return ret_val


if __name__ == "__main__":
    project_id = ""
    token = ""
    core_api = CoreApi(core_api_host="alpha-6.fabric-testbed.net", token=token)

    quotas = core_api.list_quotas(project_uuid=project_id)
    print(f"Fetching quotas: {json.dumps(quotas, indent=4)}")
    '''
    resources = ["core", "ram", "disk"]
    if len(quotas) == 0:
        for r in resources:
            core_api.create_quota(project_uuid=project_id, resource_type=r, resource_unit="hours",
                                  quota_limit=100, quota_used=0)
            print(f"Created quota for {r}")

    for q in quotas:
        core_api.update_quota(uuid=q.get("uuid"), project_uuid=q.get("project_uuid"),
                              quota_limit=q.get("quota_limit"), quota_used=q.get("quota_used") + 1,
                              resource_type=q.get("resource_type"),
                              resource_unit=q.get("resource_unit"))
        qq = core_api.get_quota(uuid=q.get("uuid"))
        print(f"Updated quota: {qq}")

    for q in quotas:
        print(f"Deleting quota: {q.get('uuid')}")
        core_api.delete_quota(uuid=q.get("uuid"))

    quotas = core_api.list_quotas(project_uuid="74a5b28b-c1a2-4fad-882b-03362dddfa71")
    print(f"Quotas after deletion!: {quotas}")
    '''
