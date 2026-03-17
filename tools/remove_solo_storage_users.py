#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""Remove users from the storage project who don't belong to any other active project.

Reads user UUIDs from solo_storage_users.csv (produced by
check_solo_storage_users.py) and removes them from the storage project.
Runs in dry-run mode by default — pass --execute to actually remove.
"""

import argparse
import csv
import sys

from core_api import CoreApi, CoreApiError

STORAGE_PROJECT_UUID = "6b8dd6eb-4b2b-4656-b3ee-ce61f91a12b4"


def main():
    parser = argparse.ArgumentParser(
        description="Remove solo-storage users from the storage project"
    )
    parser.add_argument(
        "--token-file", required=True,
        help="Path to JSON file containing id_token"
    )
    parser.add_argument(
        "--csv-file", default="solo_storage_users.csv",
        help="CSV file with users to remove (default: solo_storage_users.csv)"
    )
    parser.add_argument(
        "--core-api-host", default="https://uis.fabric-testbed.net",
        help="Core API URL (default: https://uis.fabric-testbed.net)"
    )
    parser.add_argument(
        "--storage-project-uuid", default=STORAGE_PROJECT_UUID,
        help=f"UUID of the storage project (default: {STORAGE_PROJECT_UUID})"
    )
    parser.add_argument(
        "--batch-size", type=int, default=25,
        help="Number of users to remove per API call (default: 25)"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually remove users. Without this flag, runs in dry-run mode."
    )
    args = parser.parse_args()

    try:
        core = CoreApi(core_api_host=args.core_api_host, token_file=args.token_file)
    except CoreApiError as e:
        print(f"Error initializing API: {e}", file=sys.stderr)
        sys.exit(1)

    # Load users from CSV
    users = []
    with open(args.csv_file, newline="") as f:
        for row in csv.DictReader(f):
            users.append(row)

    if not users:
        print("No users found in CSV file. Nothing to do.")
        sys.exit(0)

    # Verify the project
    try:
        project = core.get_project(args.storage_project_uuid)
        project_name = project.get("name") or "Unknown"
    except CoreApiError as e:
        print(f"Error fetching project: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Project: {project_name} ({args.storage_project_uuid})")
    print(f"Users to remove: {len(users)}")

    if not args.execute:
        print(f"\n[DRY RUN] Would remove the following {len(users)} users:\n")
        for u in users:
            name = u.get("name") or ""
            email = u.get("email") or ""
            bastion = u.get("bastion_login") or ""
            print(f"  {name:40s} {email:45s} {bastion}")
        print(f"\n[DRY RUN] Re-run with --execute to actually remove them.")
        sys.exit(0)

    # Remove in batches
    removed = 0
    failed = 0
    for i in range(0, len(users), args.batch_size):
        batch = users[i:i + args.batch_size]
        batch_uuids = [u["uuid"] for u in batch]
        try:
            core.remove_members_from_project(args.storage_project_uuid, batch_uuids)
            removed += len(batch)
            print(f"  Removed batch {i // args.batch_size + 1}: "
                  f"{len(batch)} users ({removed}/{len(users)} total)")
        except CoreApiError as e:
            print(f"  Batch failed: {e}", file=sys.stderr)
            print(f"  Falling back to individual removes...")
            for u in batch:
                name = u.get("name") or ""
                try:
                    core.remove_members_from_project(
                        args.storage_project_uuid, [u["uuid"]]
                    )
                    removed += 1
                    print(f"    Removed: {name}")
                except CoreApiError as e2:
                    failed += 1
                    print(f"    Failed:  {name} ({u['uuid']}): {e2}",
                          file=sys.stderr)

    print(f"\nDone. Removed: {removed}, Failed: {failed}")


if __name__ == "__main__":
    main()
