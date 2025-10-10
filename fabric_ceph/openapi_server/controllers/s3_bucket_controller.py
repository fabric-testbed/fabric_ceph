from fabric_ceph.response import s3_bucket_controller as rc

def create_s3_bucket(cluster, body):  # noqa: E501
    """Create a bucket

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param create_s3_bucket_request: 
    :type create_s3_bucket_request: dict | bytes

    :rtype: Union[S3Bucket, Tuple[S3Bucket, int], Tuple[S3Bucket, int, Dict[str, str]]
    """
    return rc.create_s3_bucket(cluster, body)


def delete_s3_bucket(cluster, bucket, purge_objects=None):  # noqa: E501
    """Delete a bucket

    Optionally purge objects prior to deletion. # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str
    :param purge_objects: 
    :type purge_objects: bool

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.delete_s3_bucket(cluster, bucket, purge_objects)


def get_s3_bucket(cluster, bucket):  # noqa: E501
    """Get bucket stats/owner/placement

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[S3Bucket, Tuple[S3Bucket, int], Tuple[S3Bucket, int, Dict[str, str]]
    """
    return rc.get_s3_bucket(cluster, bucket)


def list_s3_buckets(cluster, uid=None):  # noqa: E501
    """List buckets (optionally by owner)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param uid: If provided, returns buckets owned by this user only
    :type uid: str

    :rtype: Union[S3BucketList, Tuple[S3BucketList, int], Tuple[S3BucketList, int, Dict[str, str]]
    """
    return rc.list_s3_buckets(cluster, uid)


def set_s3_bucket_quota(cluster, bucket, body):  # noqa: E501
    """Set bucket-level quota

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str
    :param s3_bucket_quota_request: 
    :type s3_bucket_quota_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.set_s3_bucket_quota(cluster, bucket, body)


def set_s3_bucket_versioning(cluster, bucket, body):  # noqa: E501
    """Enable/disable versioning

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str
    :param s3_bucket_versioning_request: 
    :type s3_bucket_versioning_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.set_s3_bucket_versioning(cluster, bucket, body)
