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
from typing import Union

import connexion
from flask import Response

from fabric_ceph.common.globals import get_globals
from fabric_ceph.response.ceph_exception import CephException
from fabric_ceph.response.cors_response import cors_401, cors_400, cors_403, cors_404, cors_500, cors_200, cors_response
from fabric_ceph.security.fabric_token import FabricToken


def get_token() -> str:
    result = None
    token = connexion.request.headers.get('Authorization', None)
    if token is not None:
        token = token.replace('Bearer ', '')
        result = f"{token}"
    return result

def authorize() -> Union[FabricToken, dict, Response]:
    token = get_token()
    globals = get_globals()

    # Validate Fabric token
    fabric_token = globals.token_validator.validate_token(token=token, verify_exp=globals.config.oauth.verify_exp)

    if not fabric_token.roles:
        return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

    return fabric_token


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

