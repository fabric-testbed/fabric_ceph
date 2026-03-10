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
        Uses the service endpoint /core-api-metrics/people-details/{uuid} which is
        accessible with a service token (no user session required).
        """
        if email is not None:
            info = self.get_user_info_by_email(email=email)
            if info is None:
                raise CoreApiError(f"No user found with email: {email}")
            return info

        if uuid is None:
            uuid = self.get_user_id()

        resp = self._req("GET", f"/core-api-metrics/people-details/{uuid}")
        logging.debug(f"GET people-details/{uuid} Response : {resp.json()}")
        payload = resp.json()
        # Service endpoint may return {results: [...]} or a flat object
        results = payload.get("results") or []
        if results:
            return results[0]
        if not results and "uuid" in payload:
            return payload
        raise CoreApiError(f"No user found with uuid: {uuid}")

    # ------------- Projects -------------

    def __get_user_projects_via_service(self, *, uuid: str) -> List[dict]:
        """
        Return user's project memberships via the service endpoint
        /core-api-metrics/events/projects-membership/{person_uuid}.
        Accessible with a service token without requiring a user session.
        """
        resp = self._req("GET", f"/core-api-metrics/events/projects-membership/{uuid}")
        payload = resp.json()
        logging.debug(f"GET projects-membership/{uuid} Response : {payload}")
        results = payload.get("results") or []
        if not results and isinstance(payload, list):
            results = payload
        return results

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

        if uuid is None:
            uuid = self.get_user_id()

        all_projects = self.__get_user_projects_via_service(uuid=uuid)

        # The service endpoint returns projects the user is a member of,
        # so no need to re-check memberships — presence in the list implies membership.
        ret_val: List[dict] = []
        now = _dt.datetime.now(tz=_dt.timezone.utc)

        for p in all_projects:
            # Filter by project_id or project_name if requested
            if specific_id and p.get("uuid") != project_id:
                continue
            if specific_name and p.get("name") != project_name:
                continue

            # Expiration check
            expires_on = p.get("expires_on")
            if expires_on:
                try:
                    expires_dt = _parse_iso_utc(expires_on)
                except Exception:
                    expires_dt = None

                if expires_dt and now > expires_dt:
                    if specific_id or specific_name:
                        raise CoreApiError(f"Project {p.get('name')} is expired!")
                    continue

            ret_val.append(p)

        if len(ret_val) == 0:
            raise CoreApiError(f"User is not a member of Project: {project_id}")

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
