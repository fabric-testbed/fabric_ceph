import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from fabric_ceph.openapi_server.models.ceph_user import CephUser  # noqa: E501
from fabric_ceph.openapi_server.models.create_or_update_user_request import CreateOrUpdateUserRequest  # noqa: E501
from fabric_ceph.openapi_server.models.export_users200_response import ExportUsers200Response  # noqa: E501
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.openapi_server import util


def create_user(body):  # noqa: E501
    """Create a CephX user (with capabilities)

     # noqa: E501

    :param create_or_update_user_request: 
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[None, Tuple[None, int], Tuple[None, int, Dict[str, str]]
    """
    create_or_update_user_request = body
    if connexion.request.is_json:
        create_or_update_user_request = CreateOrUpdateUserRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def delete_user(entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[None, Tuple[None, int], Tuple[None, int, Dict[str, str]]
    """
    return 'do some magic!'


def export_users(body):  # noqa: E501
    """Export keyring(s) for one or more CephX users

     # noqa: E501

    :param export_users_request: 
    :type export_users_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    export_users_request = body
    if connexion.request.is_json:
        export_users_request = ExportUsersRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def list_users():  # noqa: E501
    """List all CephX users

     # noqa: E501


    :rtype: Union[List[CephUser], Tuple[List[CephUser], int], Tuple[List[CephUser], int, Dict[str, str]]
    """
    return 'do some magic!'


def update_user(body):  # noqa: E501
    """Update/overwrite capabilities for a CephX user

     # noqa: E501

    :param create_or_update_user_request: 
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[None, Tuple[None, int], Tuple[None, int, Dict[str, str]]
    """
    create_or_update_user_request = body
    if connexion.request.is_json:
        create_or_update_user_request = CreateOrUpdateUserRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
