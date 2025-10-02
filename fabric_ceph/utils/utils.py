#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
from http.client import BAD_REQUEST, UNAUTHORIZED, FORBIDDEN, NOT_FOUND
from typing import Union, List, Tuple

import connexion
from flask import Response, request

from fabric_ceph.common.config import Config
from fabric_ceph.common.globals import get_globals
from fabric_ceph.external_api.core_api import CoreApi, CoreApiError
from fabric_ceph.response.ceph_exception import CephException
from fabric_ceph.response.cors_response import cors_401, cors_400, cors_403, cors_404, cors_500, cors_200, cors_response
from fabric_ceph.security import fabric_token
from fabric_ceph.security.fabric_token import FabricToken, TokenException
from fabric_ceph.utils.dash_client import DashClient


def get_token() -> str:
    result = None
    token = connexion.request.headers.get('Authorization', None)
    if token is not None:
        token = token.replace('Bearer ', '')
        result = f"{token}"
    return result

from typing import Tuple, Optional
import logging

def authorize() -> Tuple[FabricToken, bool, Optional[str]]:
    """
    Validate the caller's bearer token, determine whether they are an owner of the
    configured service project, and return the user's bastion login (if present).

    Returns:
        (fabric_token, is_owner, bastion_login)
    Raises:
        TokenException: if the token is invalid or missing required claims.
        CoreApiError: for Core API communication or response errors (unless caught below).
    """
    token = get_token()
    globals_ = get_globals()

    # Validate token (and expiry if configured)
    fabric_token = globals_.token_validator.validate_token(
        token=token,
        verify_exp=globals_.config.oauth.verify_exp,
    )
    if not fabric_token or not fabric_token.uuid:
        raise TokenException("Token is missing required 'uuid' claim.")

    # Core API client (optionally honor a configured timeout if you have one)
    timeout = getattr(getattr(globals_.config, "core_api", object()), "timeout", 15.0)
    core_api = CoreApi(core_api_host=globals_.config.core_api.host, token=token, timeout=timeout)

    # Fetch user info
    user_info = core_api.get_user_info(uuid=fabric_token.uuid) or {}
    bastion_login: Optional[str] = user_info.get("bastion_login")

    # Determine service-project ownership
    service_project_id = globals_.config.runtime.service_project
    is_owner = False
    try:
        # get_user_projects returns tags/memberships when a specific project_id is requested
        projects = core_api.get_user_projects(project_id=service_project_id, uuid=fabric_token.uuid) or []
        svc_proj = next((p for p in projects if p.get("uuid") == service_project_id), projects[0] if projects else None)
        if svc_proj:
            memberships = svc_proj.get("memberships") or {}
            is_owner = bool(memberships.get("is_owner", False))
    except CoreApiError as e:
        # If the user isn't in the service project or the project is expired,
        # CoreApi may raise; treat as not owner but proceed with token and bastion_login.
        raise CephException(str(e), http_error_code=UNAUTHORIZED)

    return fabric_token, is_owner, bastion_login



def cors_error_response(error: Union[CephException, Exception]) -> Response:
    if isinstance(error, CephException):
        if error.get_http_error_code() == BAD_REQUEST:
            return cors_400(details=str(error))
        elif error.get_http_error_code() == UNAUTHORIZED:
            return cors_401(details=str(error))
        elif error.get_http_error_code() == FORBIDDEN:
            return cors_403(details=str(error))
        elif error.get_http_error_code() == NOT_FOUND:
            return cors_404(details=str(error))
        else:
            return cors_500(details=str(error))
    else:
        return cors_500(details=str(error))


def cors_success_response(response_body) -> Response:
    return cors_200(response_body=response_body)


def ordered_cluster_names(cfg: Config) -> List[str]:
    """
    If the client provides X-Cluster: a,b,c we try those in that order.
    Otherwise we try all configured clusters in config order.
    """
    hdr = (request.headers.get("X-Cluster") or "").strip()
    if not hdr:
        return list(cfg.cluster.keys())
    wanted = [x.strip() for x in hdr.split(",") if x.strip()]
    return [n for n in wanted if n in cfg.cluster] or list(cfg.cluster.keys())


def build_clients(cfg: Config, names: List[str]) -> List[Tuple[str, DashClient]]:
    return [(name, DashClient.for_cluster(name, cfg.cluster[name])) for name in names]