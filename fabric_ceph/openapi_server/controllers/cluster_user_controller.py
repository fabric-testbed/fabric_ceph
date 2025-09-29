import connexion

import fabric_ceph.response.cluster_user_controller as rc
from fabric_ceph.openapi_server.models import CreateOrUpdateUserRequest


def create_user(body):  # noqa: E501
    """Create a CephX user (with capabilities)

     # noqa: E501

    :param create_or_update_user_request:
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.create_user(body=body)


def delete_user(entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.delete_user(entity=entity)


def export_users(body):  # noqa: E501
    """Export keyring(s) for one or more CephX users

     # noqa: E501

    :param export_users_request:
    :type export_users_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return rc.export_users(body=body)


def list_users():  # noqa: E501
    """List all CephX users

     # noqa: E501


    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    return rc.list_users()


def update_user(body):  # noqa: E501
    """Update/overwrite capabilities for a CephX user

     # noqa: E501

    :param create_or_update_user_request:
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.update_user(body=body)
