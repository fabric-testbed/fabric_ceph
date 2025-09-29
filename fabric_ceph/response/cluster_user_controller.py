import json
import logging
from typing import Dict, Any

import connexion
import requests
from flask import request

from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import Users, CephUser
from fabric_ceph.openapi_server.models.create_or_update_user_request import CreateOrUpdateUserRequest  # noqa: E501
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.response.utils import get_token, cors_success_response, cors_error_response, authorize
from fabric_ceph.utils.cluster_helper import ensure_user_across_clusters


def create_user(body: dict):  # noqa: E501
    """Create a CephX user (with capabilities)

     # noqa: E501

    :param create_or_update_user_request:
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.info("Processing CephX import/sync request")


    try:
        users = []

        cfg = globals.config
        result = ensure_user_across_clusters(cfg=cfg, user_entity=body.get('user_entity'),
                                             capabilities=body.get('capabilities'))



        response = Users()
        user = CephUser(user_entity=body.get('user_entity'),
                        capabilities=body.get('capabilities'),
                        keys=[result.get('key_ring')],)
        response.data = [user]
        response.size = len(response.data)
        response.type = "users"
        return cors_success_response(response_body=response)


    except Exception as e:
        get_globals().log.exception(e)
        return cors_error_response(error=e)

def delete_user(entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
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


    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    return 'do some magic!'


def update_user(body):  # noqa: E501
    """Update/overwrite capabilities for a CephX user

     # noqa: E501

    :param create_or_update_user_request:
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    create_or_update_user_request = body
    if connexion.request.is_json:
        create_or_update_user_request = CreateOrUpdateUserRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
