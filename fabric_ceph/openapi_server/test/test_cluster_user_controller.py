import unittest

from flask import json

from fabric_ceph.openapi_server.models.create_or_update_user_request import CreateOrUpdateUserRequest  # noqa: E501
from fabric_ceph.openapi_server.models.export_users200_response import ExportUsers200Response  # noqa: E501
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

    def test_create_user(self):
        """Test case for create_user

        Create a CephX user (with capabilities)
        """
        create_or_update_user_request = {"user_entity":"client.demo","capabilities":[{"cap":"allow rw","entity":"mds"},{"cap":"allow rw","entity":"mds"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/user',
            method='POST',
            headers=headers,
            data=json.dumps(create_or_update_user_request),
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

    def test_update_user(self):
        """Test case for update_user

        Update/overwrite capabilities for a CephX user
        """
        create_or_update_user_request = {"user_entity":"client.demo","capabilities":[{"cap":"allow rw","entity":"mds"},{"cap":"allow rw","entity":"mds"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cluster/user',
            method='PUT',
            headers=headers,
            data=json.dumps(create_or_update_user_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
