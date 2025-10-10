import connexion
from typing import Dict
from typing import Tuple
from typing import Union

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
from fabric_ceph.openapi_server import util


def create_s3_user_key(cluster, uid, body=None):  # noqa: E501
    """Create or set an access key

    If &#x60;access_key&#x60; and &#x60;secret_key&#x60; omitted, a new pair is generated. # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str
    :param create_s3_key_request: 
    :type create_s3_key_request: dict | bytes

    :rtype: Union[S3KeyPair, Tuple[S3KeyPair, int], Tuple[S3KeyPair, int, Dict[str, str]]
    """
    create_s3_key_request = body
    if connexion.request.is_json:
        create_s3_key_request = CreateS3KeyRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def delete_s3_user(cluster, uid, purge_data=None):  # noqa: E501
    """Delete RGW user

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str
    :param purge_data: Also purge buckets/objects owned by the user (dangerous)
    :type purge_data: bool

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return 'do some magic!'


def delete_s3_user_key(cluster, uid, access_key):  # noqa: E501
    """Delete an access key

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str
    :param access_key: 
    :type access_key: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return 'do some magic!'


def get_s3_quota(cluster, uid):  # noqa: E501
    """Get user &amp; bucket quota

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str

    :rtype: Union[S3Quota, Tuple[S3Quota, int], Tuple[S3Quota, int, Dict[str, str]]
    """
    return 'do some magic!'


def get_s3_user(cluster, uid):  # noqa: E501
    """Get RGW user

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str

    :rtype: Union[S3User, Tuple[S3User, int], Tuple[S3User, int, Dict[str, str]]
    """
    return 'do some magic!'


def list_s3_user_keys(cluster, uid):  # noqa: E501
    """List S3 access keys for a user

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str

    :rtype: Union[List[S3KeyPair], Tuple[List[S3KeyPair], int], Tuple[List[S3KeyPair], int, Dict[str, str]]
    """
    return 'do some magic!'


def list_s3_users(cluster, uid=None, search=None, limit=None, offset=None):  # noqa: E501
    """List RGW (S3) users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: Filter by a specific user id (exact match)
    :type uid: str
    :param search: Case-insensitive substring match on uid or display_name
    :type search: str
    :param limit: 
    :type limit: int
    :param offset: 
    :type offset: int

    :rtype: Union[S3UserList, Tuple[S3UserList, int], Tuple[S3UserList, int, Dict[str, str]]
    """
    return 'do some magic!'


def set_s3_quota(cluster, uid, body):  # noqa: E501
    """Set user and/or bucket quota

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: 
    :type uid: str
    :param s3_quota_request: 
    :type s3_quota_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    s3_quota_request = body
    if connexion.request.is_json:
        s3_quota_request = S3QuotaRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def upsert_s3_user(cluster, body):  # noqa: E501
    """Create or update an RGW user (upsert)

    Mirrors radosgw-admin user create/modify with sensible defaults. # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param create_or_update_s3_user_request: 
    :type create_or_update_s3_user_request: dict | bytes

    :rtype: Union[S3User, Tuple[S3User, int], Tuple[S3User, int, Dict[str, str]]
    """
    create_or_update_s3_user_request = body
    if connexion.request.is_json:
        create_or_update_s3_user_request = CreateOrUpdateS3UserRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
