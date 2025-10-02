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
import logging
import os
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

from fss_utils.jwt_validate import JWTValidator

from fabric_ceph.common.config import Config, get_cfg
from fabric_ceph.security.token_validator import TokenValidator

logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = lambda inst, msg, *args, **kwargs: inst.log(logging.TRACE, msg, *args, **kwargs)
logging.trace = lambda msg, *args, **kwargs: logging.log(logging.TRACE, msg, *args, **kwargs)

DEFAULT_CONFIG_PATH = os.getenv("APP_CONFIG_PATH", "config.yml")


class Globals:
    def __init__(self, path: str | Path = DEFAULT_CONFIG_PATH):
        self._config = get_cfg(path)
        self._config.logging.apply()
        self._jwt_validator = None
        self._token_validator = None
        self._log = logging.getLogger(self._config.logging.logger)

        CREDMGR_CERTS = self.config.oauth.jwks_url
        CREDMGR_KEY_REFRESH = self.config.oauth.key_refresh
        CREDMGR_TRL_REFRESH = self.config.oauth.key_refresh
        self.log.info(f'Initializing JWT Validator to use {CREDMGR_CERTS} endpoint, '
                      f'refreshing keys every {CREDMGR_KEY_REFRESH} HH:MM:SS refreshing '
                      f'token revoke list every {CREDMGR_TRL_REFRESH} HH:MM:SS')
        self._jwt_validator = JWTValidator(url=CREDMGR_CERTS,
                                           refresh_period=timedelta(hours=CREDMGR_KEY_REFRESH.hour,
                                                                    minutes=CREDMGR_KEY_REFRESH.minute,
                                                                    seconds=CREDMGR_KEY_REFRESH.second))
        from urllib.parse import urlparse
        self._token_validator = TokenValidator(credmgr_host=str(urlparse(CREDMGR_CERTS).hostname),
                                               refresh_period=timedelta(hours=CREDMGR_TRL_REFRESH.hour,
                                                                        minutes=CREDMGR_TRL_REFRESH.minute,
                                                                        seconds=CREDMGR_TRL_REFRESH.second),
                                               jwt_validator=self.jwt_validator)

    @property
    def jwt_validator(self) -> JWTValidator:
        return self._jwt_validator

    @property
    def token_validator(self) -> TokenValidator:
        return self._token_validator

    @property
    def config(self) -> Config:
        return self._config

    @property
    def log(self) -> logging.Logger:
        return self._log


@lru_cache(maxsize=1)
def get_globals(path: str | Path = DEFAULT_CONFIG_PATH) -> Globals:
    """Load once, reuse everywhere."""
    return Globals(path=path)

def init_globals(path: str | Path) -> Globals:
    """Call this once at startup if you want a non-default path or to reload."""
    get_globals.cache_clear()
    return get_globals(path)
