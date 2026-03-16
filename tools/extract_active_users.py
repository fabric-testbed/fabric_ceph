#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""Extract all active FABRIC users to a CSV file.

The /people list endpoint only returns (uuid, name, email), so this script
fetches the enriched detail record for each user from the core-api-metrics
endpoint to obtain ``bastion_login``, ``active`` status, and ``last_updated``
timestamp.  Users who have not been updated in over a year are excluded.
"""

import argparse
import csv
import datetime as _dt
import sys

from core_api import CoreApi, CoreApiError, _parse_possible_timestamp


def main():
    parser = argparse.ArgumentParser(
        description="Extract active FABRIC users to CSV"
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

        active_users.append(detail)

        if i % 100 == 0 or i == total:
            print(f"  Processed {i}/{total}, {len(active_users)} active so far...")

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for user in active_users:
            writer.writerow(user)

    print(f"\nWrote {len(active_users)} active users to {args.output}")
    print(f"Skipped: {skipped_inactive} inactive, {skipped_stale} not updated in {args.inactive_days}+ days")


if __name__ == "__main__":
    main()
