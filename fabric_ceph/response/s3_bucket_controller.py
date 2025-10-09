import connexion
from flask import current_app

from fabric_ceph.common.config import Config
from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models.create_s3_bucket_request import CreateS3BucketRequest
from fabric_ceph.openapi_server.models.s3_bucket import S3Bucket
from fabric_ceph.openapi_server.models.s3_bucket_list import S3BucketList
from fabric_ceph.openapi_server.models.s3_bucket_quota_request import S3BucketQuotaRequest
from fabric_ceph.openapi_server.models.s3_bucket_versioning_request import S3BucketVersioningRequest
from fabric_ceph.response.ceph_exception import CephException
from fabric_ceph.response.cors_response import cors_400, cors_401, cors_404, cors_500, cors_200_no_content

# Service import (see implementation below)
from fabric_ceph.utils.rgw_admin import RgwAdmin, RgwNotFound, RgwBadRequest, RgwError
from fabric_ceph.utils.utils import authorize


def _rgw(cfg: Config, cluster: str) -> RgwAdmin:
    """
    Resolve an RgwAdmin client for the given cluster/region.
    You can wire this via Flask config in app factory:
        app.config['RGW_CLIENT_FACTORY'] = lambda cluster: RgwAdmin.from_config(cluster, app.config)
    """
    factory = current_app.config.get('RGW_CLIENT_FACTORY')
    if not factory:
        # Fallback: simple constructor expecting per-cluster base URLs in config
        return RgwAdmin.from_config(cluster, current_app.config)
    return factory(cluster)

def _rgw(cfg: Config, cluster: str):
    """Small helper to validate cluster name and return a RgwAdmin."""
    factory = current_app.config.get('RGW_CLIENT_FACTORY')
    if not factory:
        # Fallback: simple constructor expecting per-cluster base URLs in config
        return RgwAdmin.from_config(cluster, cfg)
    return factory(cluster)

def _to_model_bucket(b: dict) -> S3Bucket:
    """
    Convert a service-layer bucket dict → OpenAPI S3Bucket model.
    Expected dict keys from service:
      name, owner, num_objects, size_kb, placement_rule, zonegroup, zone, versioning
    """
    return S3Bucket(
        name=b.get("name"),
        owner=b.get("owner"),
        num_objects=b.get("num_objects"),
        size_kb=b.get("size_kb"),
        placement_rule=b.get("placement_rule"),
        zonegroup=b.get("zonegroup"),
        zone=b.get("zone"),
        versioning=b.get("versioning"),
    )

def create_s3_bucket(cluster, body):  # noqa: E501
    """Create a bucket"""
    g = get_globals()
    log = g.log
    log.debug("Processing S3 create_s3_bucket request")

    try:
        fabric_token, is_operator, _ = authorize()
        # Restrict to operators; loosen if you have a project/user-based ACL to check here.
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg: Config = g.config

        req = body
        if connexion.request.is_json:
            req = CreateS3BucketRequest.from_dict(connexion.request.get_json())

        if not req.bucket or not req.uid:
            return cors_400(details="bucket and uid are required")

        client = _rgw(cfg, cluster)
        b = client.create_bucket(
            bucket=req.bucket,
            owner_uid=req.uid,
            placement_rule=req.placement_rule,
            versioning=req.versioning or "Disabled",
        )
        return _to_model_bucket(b), 201

    except RgwBadRequest as e:
        return cors_400(details=str(e))
    except RgwError as e:
        return cors_500(details=str(e))
    except Exception as e:
        return cors_500(details=str(e))


def delete_s3_bucket(cluster, bucket, purge_objects=None):  # noqa: E501
    """Delete a bucket (optionally purge objects)"""
    try:
        client = _rgw(cluster)
        client.delete_bucket(bucket=bucket, purge_objects=bool(purge_objects))
        return cors_200_no_content()

    except RgwNotFound as e:
        return cors_404(details=str(e))
    except RgwBadRequest as e:
        # Often 409 from RGW “bucket not empty”
        return cors_400(details=str(e))
    except RgwError as e:
        return cors_500(details=str(e))
    except Exception as e:
        return cors_500(details=str(e))


def get_s3_bucket(cluster, bucket):  # noqa: E501
    """Get bucket stats/owner/placement"""
    try:
        client = _rgw(cluster)
        b = client.get_bucket(bucket=bucket)
        return _to_model_bucket(b), 200

    except RgwNotFound as e:
        return cors_404(details=str(e))
    except RgwError as e:
        return cors_500(details=str(e))
    except Exception as e:
        return cors_500(details=str(e))


def list_s3_buckets(cluster, uid=None):  # noqa: E501
    """List buckets (optionally by owner)"""
    try:
        client = _rgw(cluster)
        items = client.list_buckets(owner_uid=uid)  # list of dicts
        data = [_to_model_bucket(i) for i in items]

        # Build S3BucketList wrapper with your pagination/status shape
        resp = S3BucketList(
            type="s3_buckets",
            limit=len(data),            # you can wire real paging later
            offset=0,
            size=len(data),
            total=len(data),
            status=200,
            data=[d.to_dict() for d in data]
        )
        return resp, 200

    except RgwError as e:
        return cors_500(details=str(e))
    except Exception as e:
        return cors_500(details=str(e))


def set_s3_bucket_quota(cluster, bucket, body):  # noqa: E501
    """Set bucket-level quota"""
    try:
        req = body
        if connexion.request.is_json:
            req = S3BucketQuotaRequest.from_dict(connexion.request.get_json())

        client = _rgw(cluster)
        client.set_bucket_quota(
            bucket=bucket,
            enabled=True if req.enabled is None else bool(req.enabled),
            max_size_kb=req.max_size_kb,
            max_objects=req.max_objects
        )
        return cors_200_no_content(details={"bucket": bucket, "quota": req.to_dict()}), 200

    except RgwNotFound as e:
        return cors_404(details=str(e))
    except RgwBadRequest as e:
        return cors_400(details=str(e))
    except RgwError as e:
        return cors_500(details=str(e))
    except Exception as e:
        return cors_500(details=str(e))


def set_s3_bucket_versioning(cluster, bucket, body):  # noqa: E501
    """Enable/disable versioning"""
    try:
        req = body
        if connexion.request.is_json:
            req = S3BucketVersioningRequest.from_dict(connexion.request.get_json())

        if not req.status:
            return cors_400(details="status is required (Enabled|Suspended|Disabled)")

        client = _rgw(cluster)
        client.set_bucket_versioning(bucket=bucket, status=req.status)
        return cors_200_no_content({"bucket": bucket, "versioning": req.status}), 200

    except RgwNotFound as e:
        return cors_404(details=str(e))
    except RgwBadRequest as e:
        return cors_400(details=str(e))
    except RgwError as e:
        return cors_500(details=str(e))
    except Exception as e:
        return cors_500(details=str(e))
