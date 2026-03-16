#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""Add active users from a CSV export to a FABRIC project.

Reads user UUIDs from a CSV file (default: active_users.csv produced by
extract_active_users.py) and adds them as members to the specified project.
Users already in the project are skipped.
"""

import argparse
import csv
import sys

from core_api import CoreApi, CoreApiError


def main():
    parser = argparse.ArgumentParser(
        description="Add active users from CSV to a FABRIC project"
    )
    parser.add_argument(
        "--token-file", required=True,
        help="Path to JSON file containing id_token"
    )
    parser.add_argument(
        "--project-uuid", required=True,
        help="UUID of the target project"
    )
    parser.add_argument(
        "--csv-file", default="active_users.csv",
        help="CSV file with user UUIDs (default: active_users.csv)"
    )
    parser.add_argument(
        "--core-api-host", default="https://uis.fabric-testbed.net",
        help="Core API URL (default: https://uis.fabric-testbed.net)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=25,
        help="Number of users to add per API call (default: 25)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be done without making changes"
    )
    args = parser.parse_args()

    try:
        core = CoreApi(core_api_host=args.core_api_host, token_file=args.token_file)
    except CoreApiError as e:
        print(f"Error initializing API: {e}", file=sys.stderr)
        sys.exit(1)

    # Load user UUIDs from CSV
    users = []
    with open(args.csv_file, newline="") as f:
        for row in csv.DictReader(f):
            users.append(row)

    if not users:
        print("No users found in CSV file.")
        sys.exit(0)

    print(f"Loaded {len(users)} users from {args.csv_file}")

    # Get current project members to skip duplicates
    print(f"Fetching current members of project {args.project_uuid}...")
    try:
        project = core.get_project(args.project_uuid)
        existing_members = set()
        for member in project.get("project_members", []):
            existing_members.add(member.get("uuid", ""))
        for owner in project.get("project_owners", []):
            existing_members.add(owner.get("uuid", ""))
        print(f"Project '{project.get('name', '')}' has {len(existing_members)} existing members/owners")
    except CoreApiError as e:
        print(f"Warning: could not fetch project details: {e}", file=sys.stderr)
        print("Proceeding without duplicate check...")
        existing_members = set()

    # Filter out users already in the project
    to_add = [u for u in users if u["uuid"] not in existing_members]
    skipped = len(users) - len(to_add)

    if skipped:
        print(f"Skipping {skipped} users already in the project")

    if not to_add:
        print("All users are already members. Nothing to do.")
        sys.exit(0)

    print(f"Adding {len(to_add)} users to project {args.project_uuid}...")

    if args.dry_run:
        print("\n[DRY RUN] Would add the following users:")
        for u in to_add:
            print(f"  {u['name']:40s} {u['email']:45s} {u['uuid']}")
        print(f"\n[DRY RUN] Total: {len(to_add)} users")
        sys.exit(0)

    # Add in batches
    added = 0
    failed = 0
    for i in range(0, len(to_add), args.batch_size):
        batch = to_add[i:i + args.batch_size]
        batch_uuids = [u["uuid"] for u in batch]
        try:
            core.add_members_to_project(args.project_uuid, batch_uuids)
            added += len(batch)
            print(f"  Added batch {i // args.batch_size + 1}: "
                  f"{len(batch)} users ({added}/{len(to_add)} total)")
        except CoreApiError as e:
            # Fall back to adding one at a time
            print(f"  Batch failed: {e}", file=sys.stderr)
            print(f"  Falling back to individual adds...")
            for u in batch:
                try:
                    core.add_members_to_project(args.project_uuid, [u["uuid"]])
                    added += 1
                except CoreApiError as e2:
                    failed += 1
                    print(f"  Failed to add {u['name']} ({u['uuid']}): {e2}",
                          file=sys.stderr)

    print(f"\nDone. Added: {added}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
