from fabric_ceph.response import version_controller as rc

def version_get():  # noqa: E501
    """Version

    Version # noqa: E501


    :rtype: Union[Version, Tuple[Version, int], Tuple[Version, int, Dict[str, str]]
    """
    return rc.version_get()
