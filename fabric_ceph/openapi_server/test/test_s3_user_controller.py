import unittest

from flask import json

from fabric_ceph.openapi_server.models.create_or_update_s3_user_request import CreateOrUpdateS3UserRequest  # noqa: E501
from fabric_ceph.openapi_server.models.create_s3_key_request import CreateS3KeyRequest  # noqa: E501
from fabric_ceph.openapi_server.models.s3_key_pair import S3KeyPair  # noqa: E501
from fabric_ceph.openapi_server.models.s3_quota import S3Quota  # noqa: E501
from fabric_ceph.openapi_server.models.s3_quota_request import S3QuotaRequest  # noqa: E501
from fabric_ceph.openapi_server.models.s3_user import S3User  # noqa: E501
from fabric_ceph.openapi_server.models.s3_user_list import S3UserList  # noqa: E501
from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_ceph.openapi_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestS3UserController(BaseTestCase):
    """S3UserController integration test stubs"""

    def test_create_s3_user_key(self):
        """Test case for create_s3_user_key

        Create or set an access key
        """
        create_s3_key_request = {"secret_key":"secret_key","access_key":"access_key","generate":True}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}/keys'.format(uid='uid_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_s3_key_request),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_s3_user(self):
        """Test case for delete_s3_user

        Delete RGW user
        """
        query_string = [('cluster', 'europe'),
                        ('purge_data', False)]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}'.format(uid='uid_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_s3_user_key(self):
        """Test case for delete_s3_user_key

        Delete an access key
        """
        query_string = [('cluster', 'europe'),
                        ('access_key', 'access_key_example')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}/keys'.format(uid='uid_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_s3_quota(self):
        """Test case for get_s3_quota

        Get user & bucket quota
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}/quota'.format(uid='uid_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_s3_user(self):
        """Test case for get_s3_user

        Get RGW user
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}'.format(uid='uid_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_s3_user_keys(self):
        """Test case for list_s3_user_keys

        List S3 access keys for a user
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}/keys'.format(uid='uid_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_s3_users(self):
        """Test case for list_s3_users

        List RGW (S3) users
        """
        query_string = [('cluster', 'europe'),
                        ('uid', 'uid_example'),
                        ('search', 'search_example'),
                        ('limit', 100),
                        ('offset', 0)]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user',
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_set_s3_quota(self):
        """Test case for set_s3_quota

        Set user and/or bucket quota
        """
        s3_quota_request = {"user_quota":{"max_objects":1000000,"max_size_kb":104857600,"enabled":False},"bucket_quota":{"max_objects":1000000,"max_size_kb":104857600,"enabled":False}}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user/{uid}/quota'.format(uid='uid_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(s3_quota_request),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_upsert_s3_user(self):
        """Test case for upsert_s3_user

        Create or update an RGW user (upsert)
        """
        create_or_update_s3_user_request = {"uid":"project123","system":False,"keys":[{"secret_key":"secret_key","access_key":"access_key","generate":True},{"secret_key":"secret_key","access_key":"access_key","generate":True}],"max_buckets":0,"op_mask":"op_mask","display_name":"Project 123 Service Account","email":"email","suspended":False,"caps":[{"perm":"*","type":"users"},{"perm":"*","type":"users"}]}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/user',
            method='POST',
            headers=headers,
            data=json.dumps(create_or_update_s3_user_request),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
