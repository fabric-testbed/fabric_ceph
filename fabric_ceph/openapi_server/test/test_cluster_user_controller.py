import unittest

from flask import json

from fabric_ceph.openapi_server.models.apply_user_response import ApplyUserResponse  # noqa: E501
from fabric_ceph.openapi_server.models.create_user_templated_request import CreateUserTemplatedRequest  # noqa: E501
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_ceph.openapi_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.models.users import Users  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestClusterUserController(BaseTestCase):
    """ClusterUserController integration test stubs"""

    def test_apply_user_templated(self):
        """Test case for apply_user_templated

        Upsert a CephX user with cluster-specific capabilities
        """
        create_user_templated_request = {"user_entity":"client.project123","preferred_source":"europe","template_capabilities":[{"cap":"allow rw fsname={fs} path={path}","entity":"mds"},{"cap":"allow rw fsname={fs} path={path}","entity":"mds"}],"render":{"subvol_name":"project123","group_name":"fabric_staff","fs_name":"CEPH-FS-01"},"sync_across_clusters":True}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'x_cluster': 'europe,lab',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/user',
            method='POST',
            headers=headers,
            data=json.dumps(create_user_templated_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_user(self):
        """Test case for delete_user

        Delete a CephX user
        """
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/user/{entity}'.format(entity='entity_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_export_users(self):
        """Test case for export_users

        Export keyring(s) for one or more CephX users
        """
        export_users_request = {"entities":["client.demo","client.alice"]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/user/export',
            method='POST',
            headers=headers,
            data=json.dumps(export_users_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_users(self):
        """Test case for list_users

        List all CephX users
        """
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/user',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
