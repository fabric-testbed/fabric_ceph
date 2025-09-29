import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.models.version import Version  # noqa: E501
from fabric_ceph.openapi_server import util


def version_get():  # noqa: E501
    """Version

    Version # noqa: E501


    :rtype: Union[Version, Tuple[Version, int], Tuple[Version, int, Dict[str, str]]
    """
    return 'do some magic!'
