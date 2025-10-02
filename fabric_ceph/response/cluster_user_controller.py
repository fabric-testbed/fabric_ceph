import connexion

from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import Users, CephUser, Status200OkNoContent
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.response.cors_response import cors_401
from fabric_ceph.utils.utils import cors_success_response, cors_error_response, authorize
from fabric_ceph.utils.create_ceph_user import ensure_user_across_clusters
from fabric_ceph.utils.delete_ceph_user import delete_user_across_clusters
from fabric_ceph.utils.export_ceph_user import export_users_first_success, list_users_first_success
from fabric_ceph.utils.update_ceph_user import update_user_across_clusters


def create_user(body: dict):  # noqa: E501
    """Create a CephX user (with capabilities)

     # noqa: E501

    :param create_or_update_user_request:
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX create request")

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = ensure_user_across_clusters(cfg=cfg, user_entity=body.get('user_entity'),
                                             capabilities=body.get('capabilities'))
        log.debug(f"Created CephX user: {result}")


        response = Users()
        user = CephUser(user_entity=body.get('user_entity'),
                        capabilities=body.get('capabilities'),
                        keys=[result.get('key_ring')],)
        response.data = [user]
        response.size = len(response.data)
        response.type = "users"
        return cors_success_response(response_body=response)


    except Exception as e:
        log.exception(f"Failed processing CephX create request: {e}")
        return cors_error_response(error=e)

def delete_user(entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX delete request")

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = delete_user_across_clusters(cfg=cfg, user_entity=entity)
        log.debug(f"Deleted CephX user: {entity} {result}")

        response = Status200OkNoContent()
        response.data = [result]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)


    except Exception as e:
        log.exception(f"Failed processing CephX delete request: {e}")
        return cors_error_response(error=e)

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
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX export request")

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if len(export_users_request.entities) == 1 and bastion_login.lower() not in export_users_request.entities[0].lower():
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if len(export_users_request.entities) > 1 and not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = export_users_first_success(cfg=cfg, entities=export_users_request.entities)
        log.debug(f"Exported CephX users: {result}")

        response = Users()
        user = CephUser(user_entity=body.get('user_entity'),
                        capabilities=body.get('capabilities'),
                        keys=[result.get('key_ring')],)
        response.data = [user]
        response.size = len(response.data)
        response.type = "users"
        return cors_success_response(response_body=response)
    except Exception as e:
        log.exception(f"Failed processing CephX export request: {e}")
        return cors_error_response(error=e)

def list_users():  # noqa: E501
    """List all CephX users

     # noqa: E501


    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX list request")

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = list_users_first_success(cfg=cfg)
        log.debug(f"Updated CephX list users: {result}")

        response = Users()
        response.data = [result]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)
    except Exception as e:
        log.exception(f"Failed processing CephX list request: {e}")
        return cors_error_response(error=e)


def update_user(body):  # noqa: E501
    """Update/overwrite capabilities for a CephX user

     # noqa: E501

    :param create_or_update_user_request:
    :type create_or_update_user_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX update request")

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = update_user_across_clusters(cfg=cfg, user_entity=body.get('user_entity'),
                                             capabilities=body.get('capabilities'))
        log.debug(f"Updated CephX user: {result}")

        response = Status200OkNoContent()
        response.data = [result]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)


    except Exception as e:
        log.exception(f"Failed processing CephX update request: {e}")
        return cors_error_response(error=e)