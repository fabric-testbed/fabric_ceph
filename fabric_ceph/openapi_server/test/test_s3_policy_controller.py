import unittest

from flask import json

from fabric_ceph.openapi_server.models.s3_acl import S3Acl  # noqa: E501
from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestS3PolicyController(BaseTestCase):
    """S3PolicyController integration test stubs"""

    def test_delete_s3_bucket_policy(self):
        """Test case for delete_s3_bucket_policy

        Delete bucket policy
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/policy'.format(bucket='bucket_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_s3_bucket_acl(self):
        """Test case for get_s3_bucket_acl

        Get bucket ACL (Canned or XML)
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/acl'.format(bucket='bucket_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_s3_bucket_policy(self):
        """Test case for get_s3_bucket_policy

        Get bucket policy (IAM JSON)
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/policy'.format(bucket='bucket_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    @unittest.skip("Connexion does not support multiple consumes. See https://github.com/zalando/connexion/pull/760")
    def test_put_s3_bucket_acl(self):
        """Test case for put_s3_bucket_acl

        Set bucket ACL (Canned or XML)
        """
        s3_acl = {"grants":[{"grantee":"uri:http://acs.amazonaws.com/groups/global/AllUsers","permission":"READ"},{"grantee":"uri:http://acs.amazonaws.com/groups/global/AllUsers","permission":"READ"}],"canned":"public-read"}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/acl'.format(bucket='bucket_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(s3_acl),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_put_s3_bucket_policy(self):
        """Test case for put_s3_bucket_policy

        Set/replace bucket policy (IAM JSON)
        """
        request_body = None
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/policy'.format(bucket='bucket_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(request_body),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
