#!/usr/bin/env python3
# MIT License
#
# Copyright (component) 2020 FABRIC Testbed
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
import datetime
import hashlib
import json
import logging

from fabric_cm.credmgr.credmgr_proxy import CredmgrProxy, Status
from fabric_cm.credmgr.swagger_client.rest import ApiException
from fss_utils.jwt_manager import ValidateCode
from fss_utils.jwt_validate import JWTValidator

from fabric_ceph.security.fabric_token import FabricToken, TokenException


class TokenValidator:
    """This class caches revoke list retrieved from a specified endpoint
    and uses it to validate provided tokens"""

    def __init__(self, *, credmgr_host: str, refresh_period: datetime.timedelta,
                 jwt_validator: JWTValidator):
        """ Initialize a validator with an endpoint URL presenting Token revoke list,
        a refresh period for keys expressed as datetime.timedelta and
        audience (interface.e. CI Logon client id cilogon:/client_id/1234567890).
        :param credmgr_host:
        :param refresh_period:
        """
        self.credmgr_host = credmgr_host
        assert refresh_period is None or isinstance(refresh_period, datetime.timedelta)
        self.cache_period = refresh_period
        self.trl = []
        self.trl_fetched = None
        self.credmgr_proxy = CredmgrProxy(credmgr_host=credmgr_host)
        self.jwt_validator = jwt_validator
        self.logger = logging.getLogger()

    def __fetch_token_revoke_list(self, *, project_id: str):
        """
        Fetch TRL from an endpoint and save it
        @param token token
        """
        if self.trl_fetched is not None:
            if datetime.datetime.now() < self.trl_fetched + self.cache_period:
                return

        status, trl_or_exception = self.credmgr_proxy.token_revoke_list(project_id=project_id)
        if status != Status.OK:
            self.logger.error(f"Exception: {trl_or_exception}")
            details = None
            if isinstance(trl_or_exception, ApiException) and trl_or_exception.body is not None:
                details = json.loads(trl_or_exception.body.decode('utf-8'))
                errors = details.get('errors')
                if errors is not None:
                    details = errors[0].get('details')
            raise Exception(f"Unable to fetch token revoke list: {trl_or_exception.status}:{trl_or_exception.reason}:{details}")

        self.trl_fetched = datetime.datetime.now()
        if trl_or_exception is not None:
            self.trl = trl_or_exception

    def validate_token(self, *, token, verify_exp=False) -> FabricToken:
        """
        Validate a token using a JWT Validator
        Returns the decoded token
        :param token:
        :param verify_exp:
        :return decoded token in a dictionary
        """
        result = None
        token_hash = self.generate_sha256(token=token)
        if self.jwt_validator is not None:
            code, token_or_exception = self.jwt_validator.validate_jwt(token=token, verify_exp=verify_exp)
            if code is not ValidateCode.VALID:
                raise TokenException(f"Unable to validate provided token: {code}/{token_or_exception}")

            result = FabricToken(decoded_token=token_or_exception, token_hash=token_hash)
            project_id, tags, name = result.first_project
            self.__fetch_token_revoke_list(project_id=project_id)
            if token_hash in self.trl:
                raise TokenException(f"Unauthorized: Token: {token_hash} is revoked!")
        else:
            raise TokenException("JWT Token validator not initialized, skipping validation")

        return result

    @staticmethod
    def generate_sha256(*, token: str):
        """
        Generate SHA 256 for a token
        @param token token string
        """
        # Create a new SHA256 hash object
        sha256_hash = hashlib.sha256()

        # Convert the string to bytes and update the hash object
        sha256_hash.update(token.encode('utf-8'))

        # Get the hexadecimal representation of the hash
        sha256_hex = sha256_hash.hexdigest()

        return sha256_hex