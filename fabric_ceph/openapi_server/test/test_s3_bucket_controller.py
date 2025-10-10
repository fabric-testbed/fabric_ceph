import unittest

from flask import json

from fabric_ceph.openapi_server.models.create_s3_bucket_request import CreateS3BucketRequest  # noqa: E501
from fabric_ceph.openapi_server.models.s3_bucket import S3Bucket  # noqa: E501
from fabric_ceph.openapi_server.models.s3_bucket_list import S3BucketList  # noqa: E501
from fabric_ceph.openapi_server.models.s3_bucket_quota_request import S3BucketQuotaRequest  # noqa: E501
from fabric_ceph.openapi_server.models.s3_bucket_versioning_request import S3BucketVersioningRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.test import BaseTestCase


class TestS3BucketController(BaseTestCase):
    """S3BucketController integration test stubs"""

    def test_create_s3_bucket(self):
        """Test case for create_s3_bucket

        Create a bucket
        """
        create_s3_bucket_request = {"bucket":"my-bucket","uid":"project123","versioning":"Disabled","placement_rule":"placement_rule"}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket',
            method='POST',
            headers=headers,
            data=json.dumps(create_s3_bucket_request),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_s3_bucket(self):
        """Test case for delete_s3_bucket

        Delete a bucket
        """
        query_string = [('cluster', 'europe'),
                        ('purge_objects', False)]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}'.format(bucket='bucket_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_s3_bucket(self):
        """Test case for get_s3_bucket

        Get bucket stats/owner/placement
        """
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}'.format(bucket='bucket_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_s3_buckets(self):
        """Test case for list_s3_buckets

        List buckets (optionally by owner)
        """
        query_string = [('cluster', 'europe'),
                        ('uid', 'uid_example')]
        headers = { 
            'Accept': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket',
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_set_s3_bucket_quota(self):
        """Test case for set_s3_bucket_quota

        Set bucket-level quota
        """
        s3_bucket_quota_request = {"max_objects":6,"max_size_kb":0,"enabled":True}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/quota'.format(bucket='bucket_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(s3_bucket_quota_request),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_set_s3_bucket_versioning(self):
        """Test case for set_s3_bucket_versioning

        Enable/disable versioning
        """
        s3_bucket_versioning_request = {"status":"Enabled"}
        query_string = [('cluster', 'europe')]
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '/s3/bucket/{bucket}/versioning'.format(bucket='bucket_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(s3_bucket_versioning_request),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
