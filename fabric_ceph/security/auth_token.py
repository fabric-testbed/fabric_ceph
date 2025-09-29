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


class AuthToken:
    """
    Represents the Authentication Token for an actor
    """

    def __init__(self, *, name: str = None, guid: str = None, oidc_sub_claim: str = None,
                 email: str = None, token: str = None):
        self.name = name
        self.guid = guid
        self.oidc_sub_claim = oidc_sub_claim
        self.email = email
        self.token = token

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['token']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.token = None

    def get_oidc_sub_claim(self) -> str:
        """
        Get OIDC Sub Claim
        @return oidc sub claim
        """
        return self.oidc_sub_claim

    def set_oidc_sub_claim(self, oidc_sub_claim: str):
        """
        Set OIDC Sub Claim
        @param oidc_sub_claim oidc_sub_claim
        """
        self.oidc_sub_claim = oidc_sub_claim

    def set_email(self, email: str):
        """
        Set email
        @param email email
        """
        self.email = email

    def get_email(self) -> str:
        """
        Get email
        @return email
        """
        return self.email

    def get_name(self) -> str:
        """
        Return Name
        @return name
        """
        return self.name

    def get_guid(self) -> str:
        """
        Return Guid
        @return Guid
        """
        return self.guid

    def clone(self):
        """
        Clone
        @return AuthToken
        """
        return AuthToken(name=self.name, guid=self.guid)

    def __str__(self):
        return f"name: {self.name} guid: {self.guid} email: {self.email}"

    def get_token(self):
        """
        Get Token
        @return token
        """
        return self.token

    def set_token(self, token: str):
        """
        Set Token
        @param token token
        """
        self.token = token
