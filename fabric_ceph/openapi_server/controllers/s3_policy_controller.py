from fabric_ceph.response import s3_policy_controller as rc


def delete_s3_bucket_policy(cluster, bucket):  # noqa: E501
    """Delete bucket policy

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.delete_s3_bucket_policy(cluster, bucket)


def get_s3_bucket_acl(cluster, bucket):  # noqa: E501
    """Get bucket ACL (Canned or XML)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[S3Acl, Tuple[S3Acl, int], Tuple[S3Acl, int, Dict[str, str]]
    """
    return rc.get_s3_bucket_acl(cluster, bucket)


def get_s3_bucket_policy(cluster, bucket):  # noqa: E501
    """Get bucket policy (IAM JSON)

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param bucket: 
    :type bucket: str

    :rtype: Union[Dict[str, object], Tuple[Dict[str, object], int], Tuple[Dict[str, object], int, Dict[str, str]]
    """
    return rc.get_s3_bucket_policy(cluster, bucket)


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
    return rc.put_s3_bucket_acl(cluster, bucket, body)


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
    return rc.put_s3_bucket_policy(cluster, bucket, body)
