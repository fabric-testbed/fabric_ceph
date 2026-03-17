#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""Delete CephFS subvolumes and CephX users for solo-storage users.

Cross-references solo_storage_users.csv (bastion_login) with per-cluster
user CSVs to determine which cluster each user is on, then calls the
Ceph Manager API to delete their subvolume and CephX user entity.

Runs in dry-run mode by default — pass --execute to actually delete.
"""

import argparse
import csv
import sys
from collections import defaultdict

from fabric_ceph_client.fabric_ceph_client import CephManagerClient, ApiError

VOL_NAME = "CEPH-FS-01"
SUBVOL_GROUP = "fabric_users"

CLUSTERS = [
    ("east", "Ceph_FABRIC_v3_east", "users_east.csv"),
    ("west", "Ceph_FABRIC_v3_west", "users_west.csv"),
    ("europe", "Ceph_FABRIC_v3_europe", "users_europe.csv"),
    ("asia", "Ceph_FABRIC_v3_asia", "users_asia.csv"),
]


def load_cluster_users(cluster_csv_dir):
    """Load per-cluster CSVs and return {bastion_login: cluster_name}."""
    login_to_cluster = {}
    for cluster_name, subdir, csv_file in CLUSTERS:
        path = f"{cluster_csv_dir}/{subdir}/{csv_file}"
        try:
            with open(path, newline="") as f:
                for row in csv.DictReader(f):
                    name = row.get("name", "").strip()
                    if name:
                        login_to_cluster[name] = cluster_name
        except FileNotFoundError:
            print(f"  Warning: {path} not found, skipping", file=sys.stderr)
    return login_to_cluster


def main():
    parser = argparse.ArgumentParser(
        description="Delete CephFS subvolumes and CephX users for solo-storage users"
    )
    parser.add_argument(
        "--token-file", required=True,
        help="Path to JSON file containing id_token for Ceph Manager API"
    )
    parser.add_argument(
        "--csv-file", default="solo_storage_users.csv",
        help="CSV file with solo-storage users (default: solo_storage_users.csv)"
    )
    parser.add_argument(
        "--cluster-csv-dir", required=True,
        help="Directory containing per-cluster subdirs (e.g. ~/claude-jh/ceph)"
    )
    parser.add_argument(
        "--ceph-manager-url", required=True,
        help="Ceph Manager API base URL (e.g. https://ceph-manager.fabric-testbed.net)"
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
        "--force", action="store_true",
        help="Force-delete subvolumes even if not empty"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually delete. Without this flag, runs in dry-run mode."
    )
    args = parser.parse_args()

    # Load solo-storage users
    solo_users = []
    with open(args.csv_file, newline="") as f:
        for row in csv.DictReader(f):
            solo_users.append(row)

    if not solo_users:
        print("No users found in CSV file. Nothing to do.")
        sys.exit(0)

    # Build bastion_login -> cluster mapping from per-cluster CSVs
    login_to_cluster = load_cluster_users(args.cluster_csv_dir)

    # Match solo users to their clusters
    by_cluster = defaultdict(list)
    not_found = []
    for user in solo_users:
        bastion = user.get("bastion_login", "").strip()
        if not bastion:
            continue
        cluster = login_to_cluster.get(bastion)
        if cluster:
            by_cluster[cluster].append(user)
        else:
            not_found.append(user)

    # Summary
    print(f"Solo-storage users: {len(solo_users)}")
    print(f"Matched to clusters: {sum(len(v) for v in by_cluster.values())}")
    if not_found:
        print(f"Not found in any cluster CSV: {len(not_found)}")
        for u in not_found:
            print(f"  {u.get('name') or '':40s} {u.get('bastion_login') or ''}")

    for cluster_name in ["east", "west", "europe", "asia"]:
        users = by_cluster.get(cluster_name, [])
        print(f"\n  {cluster_name}: {len(users)} users")

    if not args.execute:
        print(f"\n[DRY RUN] Would delete the following subvolumes and CephX users:\n")
        for cluster_name in ["east", "west", "europe", "asia"]:
            users = by_cluster.get(cluster_name, [])
            if not users:
                continue
            print(f"  --- {cluster_name} ({len(users)} users) ---")
            for u in users:
                bastion = u.get("bastion_login") or ""
                name = u.get("name") or ""
                print(f"    subvolume: {args.vol_name}/{args.group_name}/{bastion}  "
                      f"entity: client.{bastion}  ({name})")
        print(f"\n[DRY RUN] Re-run with --execute to actually delete them.")
        sys.exit(0)

    # Initialize Ceph Manager client
    client = CephManagerClient(
        base_url=args.ceph_manager_url,
        token_file=args.token_file,
    )

    deleted_subvols = 0
    deleted_users = 0
    failed = 0

    for cluster_name in ["east", "west", "europe", "asia"]:
        users = by_cluster.get(cluster_name, [])
        if not users:
            continue

        print(f"\n--- {cluster_name}: deleting {len(users)} users ---")
        for i, u in enumerate(users, 1):
            bastion = u.get("bastion_login") or ""
            name = u.get("name") or ""
            entity = f"client.{bastion}"

            # Delete subvolume
            try:
                client.delete_subvolume(
                    cluster=cluster_name,
                    vol_name=args.vol_name,
                    subvol_name=bastion,
                    group_name=args.group_name,
                    force=args.force,
                )
                deleted_subvols += 1
                print(f"  [{i}/{len(users)}] Deleted subvolume: {bastion}")
            except ApiError as e:
                if e.status == 404:
                    print(f"  [{i}/{len(users)}] Subvolume not found (already deleted?): {bastion}")
                else:
                    failed += 1
                    print(f"  [{i}/{len(users)}] Failed to delete subvolume {bastion}: {e}",
                          file=sys.stderr)

            # Delete CephX user
            try:
                client.delete_user(cluster=cluster_name, entity=entity)
                deleted_users += 1
                print(f"  [{i}/{len(users)}] Deleted CephX user: {entity}")
            except ApiError as e:
                if e.status == 404:
                    print(f"  [{i}/{len(users)}] CephX user not found (already deleted?): {entity}")
                else:
                    failed += 1
                    print(f"  [{i}/{len(users)}] Failed to delete CephX user {entity}: {e}",
                          file=sys.stderr)

    print(f"\nDone. Deleted subvolumes: {deleted_subvols}, Deleted CephX users: {deleted_users}, "
          f"Failed: {failed}")


if __name__ == "__main__":
    main()
