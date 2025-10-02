import unittest

from flask import json

from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.models.version import Version  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestVersionController(BaseTestCase):
    """VersionController integration test stubs"""

    def test_version_get(self):
        """Test case for version_get

        Version
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/version',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
