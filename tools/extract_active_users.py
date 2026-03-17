#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""Extract active FABRIC users who belong to at least one active project
(other than the FABRIC Ceph storage service project) to a CSV file.

The /people list endpoint only returns (uuid, name, email), so this script
fetches the enriched detail record for each user from the core-api-metrics
endpoint to obtain ``bastion_login``, ``active`` status, and ``last_updated``
timestamp.  Users who have not been updated in over a year are excluded.
Users whose only active project is the Ceph storage project are excluded.

A user qualifies if they are a member, owner, or creator of at least one
active project other than the Ceph storage project.
"""

import argparse
import csv
import datetime as _dt
import sys

from core_api import CoreApi, CoreApiError, _parse_possible_timestamp

STORAGE_PROJECT_UUID = "6b8dd6eb-4b2b-4656-b3ee-ce61f91a12b4"


def main():
    parser = argparse.ArgumentParser(
        description="Extract active FABRIC users (with non-storage project membership) to CSV"
    )
    parser.add_argument(
        "--token-file", required=True,
        help="Path to JSON file containing id_token"
    )
    parser.add_argument(
        "--output", default="active_users.csv",
        help="Output CSV path (default: active_users.csv)"
    )
    parser.add_argument(
        "--core-api-host", default="https://uis.fabric-testbed.net",
        help="Core API URL (default: https://uis.fabric-testbed.net)"
    )
    parser.add_argument(
        "--inactive-days", type=int, default=365,
        help="Exclude users not updated in this many days (default: 365)"
    )
    parser.add_argument(
        "--storage-project-uuid", default=STORAGE_PROJECT_UUID,
        help=f"UUID of the Ceph storage project to exclude (default: {STORAGE_PROJECT_UUID})"
    )
    args = parser.parse_args()

    try:
        core = CoreApi(core_api_host=args.core_api_host, token_file=args.token_file)
    except CoreApiError as e:
        print(f"Error initializing API: {e}", file=sys.stderr)
        sys.exit(1)

    # Collect all people (list endpoint: uuid, name, email only)
    print("Fetching user list...")
    try:
        people = core.collect_people()
    except CoreApiError as e:
        print(f"Error fetching people: {e}", file=sys.stderr)
        sys.exit(1)

    total = len(people)
    cutoff = _dt.datetime.now(tz=_dt.timezone.utc) - _dt.timedelta(days=args.inactive_days)
    print(f"Found {total} users. Fetching details (excluding inactive > {args.inactive_days} days)...")

    # Fetch enriched detail for each user
    fields = ["uuid", "name", "email", "bastion_login"]
    active_users = []
    skipped_inactive = 0
    skipped_stale = 0
    skipped_no_project = 0

    for i, person in enumerate(people, 1):
        uuid = person.get("uuid")
        if not uuid:
            continue
        try:
            detail = core.get_person_details(uuid)
        except CoreApiError:
            # Metrics endpoint can 500 for users with missing org data;
            # fall back to the regular detail endpoint.
            try:
                detail = core.get_person(uuid)
            except CoreApiError as e2:
                print(f"  Warning: could not fetch detail for {uuid}: {e2}",
                      file=sys.stderr)
                continue

        # Must be active
        if not core._is_active(detail):
            skipped_inactive += 1
            if i % 100 == 0 or i == total:
                print(f"  Processed {i}/{total}...")
            continue

        # Must have been updated within the cutoff
        last_updated = _parse_possible_timestamp(detail.get("last_updated") or "")
        if last_updated and last_updated < cutoff:
            skipped_stale += 1
            if i % 100 == 0 or i == total:
                print(f"  Processed {i}/{total}...")
            continue

        # Must be a member, owner, or creator of at least one active project
        # other than the storage project
        try:
            seen_uuids = set()
            other_projects = []
            for role in ("member", "creator", "owner"):
                try:
                    projects = core.collect_projects(
                        person_uuid=uuid, active=True,
                        extra={"role": role},
                    )
                except CoreApiError:
                    # Some API versions may not support role filter; fall back
                    if role == "member":
                        projects = core.collect_projects(
                            person_uuid=uuid, active=True,
                        )
                    else:
                        continue
                for p in projects:
                    puuid = p.get("uuid")
                    if puuid and puuid != args.storage_project_uuid and puuid not in seen_uuids:
                        seen_uuids.add(puuid)
                        other_projects.append(p)
            if not other_projects:
                skipped_no_project += 1
                name = detail.get("name") or ""
                email = detail.get("email") or ""
                print(f"  Skipping {name} ({email}) — no active project besides Ceph storage")
                if i % 100 == 0 or i == total:
                    print(f"  Processed {i}/{total}...")
                continue
        except CoreApiError as e:
            # If we can't check projects, include the user to be safe
            print(f"  Warning: could not check projects for {uuid}: {e}",
                  file=sys.stderr)

        active_users.append(detail)

        if i % 100 == 0 or i == total:
            print(f"  Processed {i}/{total}, {len(active_users)} active so far...")

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for user in active_users:
            writer.writerow(user)

    print(f"\nWrote {len(active_users)} active users to {args.output}")
    print(f"Skipped: {skipped_inactive} inactive, {skipped_stale} stale (>{args.inactive_days} days), "
          f"{skipped_no_project} no active project besides Ceph storage")


if __name__ == "__main__":
    main()
