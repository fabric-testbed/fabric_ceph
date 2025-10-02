#!/usr/bin/env python3
# MIT License
#
# Copyright (component) 2025 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
from dataclasses import dataclass
from typing import List, Dict

from fabric_ceph.common.config import Config
from fabric_ceph.utils.dash_client import DashClient


@dataclass
class DeleteResult:
    entity: str
    deleted_from: List[str]
    not_found: List[str]
    errors: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "entity": self.entity,
            "deleted_from": self.deleted_from,
            "not_found": self.not_found,
            "errors": self.errors,
        }

def delete_user_across_clusters(
    cfg: Config,
    user_entity: str,
) -> Dict[str, object]:
    """
    Try to delete `user_entity` from every cluster.
    Returns a summary with per-cluster outcomes.
    """
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    deleted_from: List[str] = []
    not_found: List[str] = []
    errors: Dict[str, str] = {}

    for name, dc in clients.items():
        try:
            ok, msg = dc.delete_user(user_entity)
            if ok:
                deleted_from.append(name)
            else:
                not_found.append(name)
        except Exception as e:
            errors[name] = str(e)

    return DeleteResult(
        entity=user_entity,
        deleted_from=deleted_from,
        not_found=not_found,
        errors=errors,
    ).to_dict()