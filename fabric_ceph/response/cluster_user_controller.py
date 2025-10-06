from typing import Dict

import connexion

from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import Users, CephUser, Status200OkNoContentData, Status200OkNoContent, \
    ApplyUserResponse, ExportUsersResponse
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.response.cors_response import cors_401, cors_400, cors_500, cors_200
from fabric_ceph.utils.utils import cors_success_response, cors_error_response, authorize
from fabric_ceph.utils.create_ceph_user import ensure_user_across_clusters_with_cluster_paths
from fabric_ceph.utils.delete_ceph_user import delete_user_across_clusters
from fabric_ceph.utils.export_ceph_user import export_users_first_success, list_users_first_success


def apply_user_templated(body: Dict, x_cluster=None):  # operationId: applyUserTemplated
    g = get_globals()
    log = g.log
    try:
        log.debug("Applying user templated")
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            log.error(f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if connexion.request.is_json:
            body = connexion.request.get_json()

        user_entity   = body["user_entity"]
        tmpl_caps     = body["template_capabilities"]
        render_single = body.get("render")
        render_multi  = body.get("renders")  # optional list
        sync          = bool(body.get("sync_across_clusters", True))
        preferred     = body.get("preferred_source")

        # Validate template caps
        if not tmpl_caps or not isinstance(tmpl_caps, list):
            log.error("template_capabilities must be a non-empty list")
            return cors_400(details="template_capabilities must be a non-empty list")

        # Normalize render context: support either 'render' (dict) or 'renders' (list)
        if render_single and isinstance(render_single, dict):
            ctx = render_single
        elif render_multi and isinstance(render_multi, list) and len(render_multi) > 0:
            # TEMP: consume the first context until multi-path server support is implemented
            ctx = render_multi[0]
            log.warning("Received 'renders' list; using the first item for now. "
                        "Add a multi-path helper to apply all contexts.")
        else:
            log.error("render or renders[] is required")
            return cors_400(details="Either 'render' (object) or 'renders' (non-empty list) is required")

        # Validate ctx
        if "fs_name" not in ctx or "subvol_name" not in ctx:
            log.error("render.fs_name and render.subvol_name are required")
            return cors_400(details="render.fs_name and render.subvol_name are required")

        cfg = g.config

        # Apply using the existing single-context helper
        summary = ensure_user_across_clusters_with_cluster_paths(
            cfg=cfg,
            user_entity=user_entity,
            base_capabilities=tmpl_caps,
            fs_name=ctx["fs_name"],
            subvol_name=ctx["subvol_name"],
            group_name=ctx.get("group_name"),
            preferred_source=preferred,
        )

        errors: dict = (summary.get("errors") or {})
        if errors:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            log.error(f"Failed to apply templated: {details}")
            return cors_500(details=details)

        response = ApplyUserResponse.from_dict({
            "user_entity": user_entity,
            "fs_name": ctx["fs_name"],
            "subvol_name": ctx["subvol_name"],
            "group_name": ctx.get("group_name"),
            "source_cluster": summary.get("source_cluster"),
            "created_on_source": summary.get("created_on_source"),
            "updated_on_source": summary.get("updated_on_source"),
            "imported_to": summary.get("imported_to", []),
            "caps_applied": summary.get("caps_applied", {}),
            "paths": summary.get("paths", {}),
            "errors": summary.get("errors", {}),
        })
        return cors_success_response(response_body=response)

    except Exception as e:
        g.log.exception(e)
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

        errors: dict = (result.get("errors") or {})
        any_error = bool(errors)
        if any_error:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            return cors_500(details=details)

        user_info = Status200OkNoContentData()
        user_info.message = f"Subvolume {user_info} deleted."
        user_info.details = result
        response = Status200OkNoContent()
        response.data = [user_info]
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

    :rtype: Union[ExportUsersResponse, Tuple[ExportUsersResponse, int], Tuple[ExportUsersResponse, int, Dict[str, str]]
    """
    export_users_request = body
    if connexion.request.is_json:
        export_users_request = ExportUsersRequest.from_dict(connexion.request.get_json())  # noqa: E501
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX export request")
    keyring_only = True

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if len(export_users_request.entities) == 1 and bastion_login.lower() not in export_users_request.entities[0].lower():
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if len(export_users_request.entities) > 1 and not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if is_operator:
            keyring_only = False

        cfg = globals.config
        clusters = export_users_first_success(cfg=cfg, entities=export_users_request.entities,
                                              keyring_only=keyring_only)
        log.debug(f"Exported CephX users: {clusters}")

        response = ExportUsersResponse()
        response.clusters = clusters
        response.size = len(response.clusters)
        response.status = 200
        response.type = "keyring"
        return cors_success_response(response_body=response)
    except Exception as e:
        log.exception(f"Failed processing CephX export request: {e}")
        return cors_error_response(error=e)

def list_users():
    g = get_globals()
    log = g.log
    log.debug("Processing CephX list request")
    try:
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        result = list_users_first_success(cfg=g.config)  # {"cluster": "...", "users": [...]}
        raw_users = result.get("users", [])
        users = [CephUser.from_dict(u) for u in raw_users]

        resp = Users()
        resp.data = users
        resp.size = len(users)
        resp.status = 200
        resp.type = "users"
        return cors_200(response_body=resp)
    except Exception as e:
        log.exception(f"Failed processing CephX list request: {e}")
        return cors_error_response(error=e)


if __name__ == "__main__":

    temp = {'entity': 'client.ceph-exporter.hawi-osd', 'caps': {'mgr': 'allow r', 'mon': 'profile ceph-exporter', 'osd': 'allow r'}, 'key': '***********'}
    abc = CephUser.from_dict(temp)
    print(abc.to_dict())