import unittest

from flask import json

from fabric_ceph.openapi_server.models.cluster_info_list import ClusterInfoList  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_ceph.openapi_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestClusterInfoController(BaseTestCase):
    """ClusterInfoController integration test stubs"""

    def test_list_cluster_info(self):
        """Test case for list_cluster_info

        Get FSID and monitor endpoints for all clusters
        """
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/info',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
