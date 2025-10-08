from typing import Dict, List, Any

import connexion

from fabric_ceph.common.config import Config
from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import (
    Users,
    CephUser,
    Status200OkNoContentData,
    Status200OkNoContent,
    ApplyUserResponse,
    ExportUsersResponse,
)
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.response.cors_response import cors_401, cors_400, cors_500, cors_200
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.utils import cors_success_response, cors_error_response, authorize, normalize_kv_caps

# UPDATED: per-cluster helper imports
from fabric_ceph.utils.cluster_user_helper import (
    ensure_user_on_cluster_with_cluster_paths_multi,
    delete_user_on_cluster,
    list_users_on_cluster,
    export_users_on_cluster,
)


def apply_user_templated(cluster, body):  # noqa: E501
    """Upsert a CephX user with cluster-specific capabilities

    Creates or updates a CephX user and synchronizes the SAME SECRET across all clusters. Capability strings may include placeholders &#x60;{fs}&#x60;, &#x60;{path}&#x60;, &#x60;{group}&#x60;, &#x60;{subvol}&#x60; which are rendered per cluster using subvolume info (getpath).  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param create_user_templated_request:
    :type create_user_templated_request: dict | bytes

    :rtype: Union[ApplyUserResponse, Tuple[ApplyUserResponse, int], Tuple[ApplyUserResponse, int, Dict[str, str]]
    """
    g = get_globals()
    log = g.log
    try:
        fabric_token, is_operator, _ = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if connexion.request.is_json:
            body = connexion.request.get_json()

        user_entity = body["user_entity"]
        tmpl_caps   = body["template_capabilities"]
        renders     = body.get("renders")  # REQUIRED: list of {fs_name, subvol_name, [group_name]}

        if not isinstance(tmpl_caps, list) or not tmpl_caps:
            return cors_400(details="template_capabilities must be a non-empty list")
        if not isinstance(renders, list) or not renders:
            return cors_400(details="'renders' must be a non-empty list of {fs_name, subvol_name, [group_name]}")

        # Validate each render context
        bad = [r for r in renders if not isinstance(r, dict) or "fs_name" not in r or "subvol_name" not in r]
        if bad:
            return cors_400(details="each item in 'renders' must include fs_name and subvol_name")

        summary = ensure_user_on_cluster_with_cluster_paths_multi(
            cfg=g.config,
            cluster=cluster,
            user_entity=user_entity,
            base_capabilities=tmpl_caps,
            renders=renders,  # apply ALL contexts for this cluster
        )

        errors = summary.get("errors") or {}
        if errors:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            log.error("apply_user_templated (per-cluster) had errors: %s", details)
            return cors_500(details=details)

        # For top-level fields in ApplyUserResponse, use the FIRST render as representative
        first = renders[0]
        response = ApplyUserResponse.from_dict({
            "user_entity": user_entity,
            "fs_name": first["fs_name"],
            "subvol_name": first["subvol_name"],
            "group_name": first.get("group_name"),
            "source_cluster": summary.get("source_cluster"),
            "created_on_source": summary.get("created_on_source"),
            "updated_on_source": summary.get("updated_on_source"),
            "imported_to": summary.get("imported_to", []),   # will be empty in per-cluster mode
            "caps_applied": summary.get("caps_applied", {}), # { cluster: [ {entity,cap}, ... ] }
            "paths": summary.get("paths", {}),               # { cluster: "<first path>" }
            "errors": summary.get("errors", {}),
        })
        return cors_success_response(response_body=response)

    except Exception as e:
        g.log.exception(e)
        return cors_error_response(error=e)


def delete_user(cluster, entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephX delete request")

    try:
        fabric_token, is_operator, _ = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = delete_user_on_cluster(cfg=cfg, cluster=cluster, user_entity=entity)
        log.debug(f"Deleted CephX user: {entity} on {cluster}: {result}")

        errors: dict = (result.get("errors") or {})
        if errors:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            return cors_500(details=details)

        user_info = Status200OkNoContentData()
        user_info.message = f"User {entity} deleted."
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

def export_users(cluster, body):  # noqa: E501
    """Export keyring(s) for one or more CephX users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
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
        if not is_operator and len(export_users_request.entities) == 1 and bastion_login.lower() not in export_users_request.entities[0].lower():
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if len(export_users_request.entities) > 1 and not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        if is_operator:
            keyring_only = False

        cfg = globals.config
        per_cluster = export_users_on_cluster(
            cfg=cfg,
            cluster=cluster,
            entities=export_users_request.entities,
            keyring_only=keyring_only,
        )
        # Shape into ExportUsersResponse: clusters := { cluster: {entity: keyring_or_key} }
        clusters_map = {cluster: per_cluster.get("entities", {})}
        log.debug(f"Exported CephX users from {cluster}: {clusters_map}")

        response = ExportUsersResponse()
        response.clusters = clusters_map
        response.size = len(response.clusters)
        response.status = 200
        response.type = "keyrings"
        return cors_success_response(response_body=response)
    except Exception as e:
        log.exception(f"Failed processing CephX export request: {e}")
        return cors_error_response(error=e)

def _raw_user_to_ceph_user(raw: Dict[str, Any]) -> CephUser:
    # entity name
    user_entity = raw.get("user_entity") or raw.get("entity") or raw.get("id") or ""

    # capabilities: accept either a list (already normalized) or a dict mapping
    if isinstance(raw.get("capabilities"), list):
        caps_list: List[Dict[str, str]] = raw["capabilities"]
    else:
        caps_map: Dict[str, str] = raw.get("caps") or {}
        caps_list = [{"entity": ent, "cap": cap} for ent, cap in caps_map.items()]

    # optional metadata: carry through anything useful (avoid the secret itself)
    meta: Dict[str, Any] = {}
    if "key" in raw:
        meta["has_key"] = True  # don’t expose the masked key in the model
    # include any extra fields except known ones
    for k in raw.keys() - {"entity", "user_entity", "id", "caps", "capabilities", "key", "keys"}:
        meta[k] = raw[k]

    payload: Dict[str, Any] = {
        "user_entity": user_entity,
        "capabilities": caps_list,
    }
    if meta:
        payload["metadata"] = meta
    # keys are optional; omit rather than sending a masked/meaningless value
    return CephUser.from_dict(payload)

def list_users(cluster):  # noqa: E501
    """List all CephX users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str

    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    g = get_globals()
    log = g.log
    log.debug("Processing CephX list request")

    try:
        fabric_token, is_operator, _ = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        result = list_users_on_cluster(cfg=g.config, cluster=cluster)  # {"cluster": "...", "users": [...]}
        raw_users = result.get("users", [])
        users = [_raw_user_to_ceph_user(u) for u in raw_users]

        resp = Users()
        resp.data = users
        resp.size = len(users)
        resp.status = 200
        resp.type = "users"
        return cors_200(response_body=resp)
    except Exception as e:
        log.exception(f"Failed processing CephX list request: {e}")
        return cors_error_response(error=e)

def overwrite_user_caps(cluster, body):  # noqa: E501
    """Overwrite capabilities for an existing CephX user (non-templated)

    Overwrites a user&#39;s capabilities with the provided list. Commonly used to adjust a single component (e.g., &#x60;mds&#x60;) while preserving others (&#x60;mon&#x60;, &#x60;osd&#x60;, &#x60;mgr&#x60;).  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param update_user_caps_request:
    :type update_user_caps_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    g = get_globals()
    log = g.log
    log.debug("Processing CephX overwrite caps request")

    try:
        fabric_token, is_operator, _ = authorize()
        if not is_operator:
            log.error(f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        # Parse/normalize body
        if connexion.request.is_json:
            body = connexion.request.get_json()

        if not isinstance(body, dict):
            log.error(f"Failed processing CephX overwrite caps request: {body}")
            return cors_400(details="Request body must be JSON object")

        user_entity = body.get("user_entity")
        caps_in = body.get("capabilities")

        if not user_entity or not isinstance(user_entity, str):
            log.error(f"Failed processing CephX overwrite caps request: {user_entity}")
            return cors_400(details="'user_entity' is required and must be a string")

        # Accept either:
        #  - list[{"type": "mds", "value": "allow ..."}]  (preferred; matches OpenAPI)
        #  - dict {"mds": "allow ...", "mon": "..."}      (we'll normalize it)
        capabilities: List[Dict[str, str]] = []
        if isinstance(caps_in, list):
            # Validate items
            for item in caps_in:
                if not isinstance(item, dict) or "type" not in item or "value" not in item:
                    return cors_400(details="Each capability must have 'type' and 'value'")
                capabilities.append({"type": str(item["type"]), "value": str(item["value"])})
        elif isinstance(caps_in, dict):
            capabilities = [{"type": k, "value": v} for k, v in caps_in.items()]
        else:
            log.error(f"Failed processing CephX overwrite caps request: {caps_in}")
            return cors_400(details="'capabilities' must be a list of {type,value} or a dict of component->rule")

        # Validate cluster & build client
        cfg: Config = g.config
        if cluster not in cfg.cluster:
            log.error(f"Failed processing CephX overwrite caps request: {cluster}")
            return cors_400(details=f"Unknown cluster '{cluster}'")

        dc = DashClient.for_cluster(cluster, cfg.cluster[cluster])

        # PUT to Dashboard (overwrites caps)
        log.debug(f"Processing CephX overwrite caps request entity: {user_entity} capabilities: {capabilities}")
        capabilities = normalize_kv_caps(capabilities)
        status, detail = dc.update_user_caps(user_entity, capabilities)
        if status not in (200, 201, 202):
            log.error(f"Failed processing CephX overwrite caps request: {status}: {detail}")
            return cors_500(details=f"Dashboard returned HTTP {status}:{detail} while updating caps")

        # Build response
        info = Status200OkNoContentData()
        info.message = f"User {user_entity} capabilities overwritten."
        info.details = {
            "cluster": cluster,
            "user_entity": user_entity,
            "capabilities": capabilities,
            "http_status": status,
        }
        resp = Status200OkNoContent()
        resp.data = [info]
        resp.size = 1
        resp.status = 200
        resp.type = "no_content"
        return cors_success_response(response_body=resp)

    except Exception as e:
        log.exception("Failed processing CephX overwrite caps request: %s", e)
        return cors_error_response(error=e)