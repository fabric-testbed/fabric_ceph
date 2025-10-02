import unittest

from flask import json

from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_ceph.openapi_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.models.subvolume_create_or_resize_request import SubvolumeCreateOrResizeRequest  # noqa: E501
from fabric_ceph.openapi_server.models.subvolume_exists import SubvolumeExists  # noqa: E501
from fabric_ceph.openapi_server.models.subvolume_info import SubvolumeInfo  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestCephFSController(BaseTestCase):
    """CephFSController integration test stubs"""

    def test_create_or_resize_subvolume(self):
        """Test case for create_or_resize_subvolume

        Create or resize a subvolume
        """
        subvolume_create_or_resize_request = {"subvol_name":"subvol_name","mode":"0777","size":10737418240,"group_name":"group_name"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cephfs/subvolume/{vol_name}'.format(vol_name='vol_name_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(subvolume_create_or_resize_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_subvolume(self):
        """Test case for delete_subvolume

        Delete a subvolume
        """
        query_string = [('subvol_name', 'subvol_name_example'),
                        ('group_name', 'group_name_example'),
                        ('force', False)]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cephfs/subvolume/{vol_name}'.format(vol_name='vol_name_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_subvolume_info(self):
        """Test case for get_subvolume_info

        Get subvolume info (path)
        """
        query_string = [('subvol_name', 'subvol_name_example'),
                        ('group_name', 'group_name_example')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cephfs/subvolume/{vol_name}/info'.format(vol_name='vol_name_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_subvolume_exists(self):
        """Test case for subvolume_exists

        Check whether a subvolume exists
        """
        query_string = [('subvol_name', 'subvol_name_example'),
                        ('group_name', 'group_name_example')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/cephfs/subvolume/{vol_name}/exists'.format(vol_name='vol_name_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
