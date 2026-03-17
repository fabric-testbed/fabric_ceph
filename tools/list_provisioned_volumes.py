#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""List provisioned CephFS subvolumes and CephX users across all clusters.

Queries the Ceph Manager API for each cluster and reports:
  - Number of subvolumes per cluster (in the specified group)
  - Number of CephX users per cluster
  - Total across all clusters
"""

import argparse
import sys

from fabric_ceph_client.fabric_ceph_client import CephManagerClient, ApiError

VOL_NAME = "CEPH-FS-01"
SUBVOL_GROUP = "fabric_users"
CLUSTERS = ["east", "west", "europe", "asia"]


def main():
    parser = argparse.ArgumentParser(
        description="List provisioned CephFS subvolumes and CephX users across clusters"
    )
    parser.add_argument(
        "--token-file", required=True,
        help="Path to JSON file containing id_token for Ceph Manager API"
    )
    parser.add_argument(
        "--ceph-manager-url", required=True,
        help="Ceph Manager API base URL"
    )
    parser.add_argument(
        "--vol-name", default=VOL_NAME,
        help=f"CephFS volume name (default: {VOL_NAME})"
    )
    parser.add_argument(
        "--group-name", default=SUBVOL_GROUP,
        help=f"CephFS subvolume group (default: {SUBVOL_GROUP})"
    )
    parser.add_argument(
        "--clusters", nargs="+", default=CLUSTERS,
        help=f"Clusters to query (default: {' '.join(CLUSTERS)})"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="List individual subvolume and user names"
    )
    args = parser.parse_args()

    client = CephManagerClient(
        base_url=args.ceph_manager_url,
        token_file=args.token_file,
    )

    total_subvols = 0
    total_users = 0

    for cluster in args.clusters:
        print(f"\n{'='*60}")
        print(f"Cluster: {cluster}")
        print(f"{'='*60}")

        # List subvolumes
        subvol_names = []
        try:
            result = client.list_subvolumes(
                cluster=cluster,
                vol_name=args.vol_name,
                group_name=args.group_name,
            )
            subvol_names = result.get("data") or result.get("results") or []
            if isinstance(subvol_names, list) and subvol_names and isinstance(subvol_names[0], dict):
                subvol_names = [s.get("name") or s.get("subvol_name") or str(s) for s in subvol_names]
            print(f"  Subvolumes ({args.vol_name}/{args.group_name}): {len(subvol_names)}")
            if args.verbose and subvol_names:
                for name in sorted(subvol_names):
                    print(f"    {name}")
        except ApiError as e:
            print(f"  Subvolumes: ERROR - {e}", file=sys.stderr)

        # List CephX users
        user_names = []
        try:
            result = client.list_users(cluster=cluster)
            user_names = result.get("data") or result.get("results") or []
            if isinstance(user_names, list) and user_names and isinstance(user_names[0], dict):
                user_names = [u.get("entity") or u.get("name") or str(u) for u in user_names]
            print(f"  CephX users: {len(user_names)}")
            if args.verbose and user_names:
                for name in sorted(user_names):
                    print(f"    {name}")
        except ApiError as e:
            print(f"  CephX users: ERROR - {e}", file=sys.stderr)

        total_subvols += len(subvol_names)
        total_users += len(user_names)

    print(f"\n{'='*60}")
    print(f"TOTAL across {len(args.clusters)} clusters:")
    print(f"  Subvolumes: {total_subvols}")
    print(f"  CephX users: {total_users}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
