import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from fabric_ceph.openapi_server.models.s3_acl import S3Acl  # noqa: E501
from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server import util


def delete_s3_bucket_policy(cluster, bucket):  # noqa: E501
    """Delete bucket policy

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return 'do some magic!'


def get_s3_bucket_acl(cluster, bucket):  # noqa: E501
    """Get bucket ACL (Canned or XML)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[S3Acl, Tuple[S3Acl, int], Tuple[S3Acl, int, Dict[str, str]]
    """
    return 'do some magic!'


def get_s3_bucket_policy(cluster, bucket):  # noqa: E501
    """Get bucket policy (IAM JSON)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[Dict[str, object], Tuple[Dict[str, object], int], Tuple[Dict[str, object], int, Dict[str, str]]
    """
    return 'do some magic!'


def put_s3_bucket_acl(cluster, bucket, body):  # noqa: E501
    """Set bucket ACL (Canned or XML)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str
    :param s3_acl: 
    :type s3_acl: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    s3_acl = body
    if connexion.request.is_json:
        s3_acl = S3Acl.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def put_s3_bucket_policy(cluster, bucket, body):  # noqa: E501
    """Set/replace bucket policy (IAM JSON)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str
    :param request_body: 
    :type request_body: Dict[str, ]

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    request_body = body
    return 'do some magic!'
