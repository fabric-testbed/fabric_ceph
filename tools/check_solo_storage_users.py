#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)

"""Find members of the storage project who don't belong to any other active project.

Fetches all members of the storage service project from the Core API,
then checks each member's other active project memberships. Users whose
only active project is the storage project are candidates for removal.
"""

import argparse
import csv
import sys

from core_api import CoreApi, CoreApiError

STORAGE_PROJECT_UUID = "6b8dd6eb-4b2b-4656-b3ee-ce61f91a12b4"


def main():
    parser = argparse.ArgumentParser(
        description="Find storage project members not in any other active project"
    )
    parser.add_argument(
        "--token-file", required=True,
        help="Path to JSON file containing id_token"
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
        "--output", default="solo_storage_users.csv",
        help="Output CSV for users to remove (default: solo_storage_users.csv)"
    )
    args = parser.parse_args()

    try:
        core = CoreApi(core_api_host=args.core_api_host, token_file=args.token_file)
    except CoreApiError as e:
        print(f"Error initializing API: {e}", file=sys.stderr)
        sys.exit(1)

    # Fetch the storage project and its members
    print(f"Fetching storage project {args.storage_project_uuid}...")
    try:
        project = core.get_project(args.storage_project_uuid)
    except CoreApiError as e:
        print(f"Error fetching project: {e}", file=sys.stderr)
        sys.exit(1)

    project_name = project.get("name", "Unknown")
    members = project.get("project_members", [])
    owners = project.get("project_owners", [])
    print(f"Project: {project_name}")
    print(f"Members: {len(members)}, Owners: {len(owners)}")

    # Combine members (skip owners — they manage the project)
    all_members = []
    owner_uuids = {o.get("uuid") for o in owners}
    for m in members:
        uuid = m.get("uuid")
        if uuid and uuid not in owner_uuids:
            all_members.append(m)

    total = len(all_members)
    print(f"\nChecking {total} non-owner members for other active projects...\n")

    solo_users = []     # only in storage project
    multi_users = []    # in other active projects too
    errors = 0

    for i, member in enumerate(all_members, 1):
        uuid = member.get("uuid") or ""
        name = member.get("name") or ""
        email = member.get("email") or ""

        try:
            # Check all roles: member, creator, owner
            seen_uuids = set()
            other_projects = []
            for role in ("member", "creator", "owner"):
                try:
                    projects = core.collect_projects(
                        person_uuid=uuid, active=True,
                        extra={"role": role},
                    )
                except CoreApiError:
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
                # Fetch bastion_login from detail endpoint
                bastion_login = ""
                try:
                    detail = core.get_person_details(uuid)
                    bastion_login = detail.get("bastion_login") or ""
                except CoreApiError:
                    try:
                        detail = core.get_person(uuid)
                        bastion_login = detail.get("bastion_login") or ""
                    except CoreApiError:
                        pass
                member["bastion_login"] = bastion_login
                solo_users.append(member)
                print(f"  REMOVE: {name:40s} {email:45s} bastion={bastion_login}")
            else:
                multi_users.append(member)
                other_names = [p.get("name", "?") for p in other_projects]
                # Only print first few to avoid flooding
                display = other_names[:3]
                suffix = f" +{len(other_names)-3} more" if len(other_names) > 3 else ""
                print(f"  KEEP:   {name:40s} {email:45s} ({len(other_projects)} other: {', '.join(display)}{suffix})")

        except CoreApiError as e:
            errors += 1
            print(f"  ERROR:  {name:40s} {email:45s} -> {e}", file=sys.stderr)

        if i % 50 == 0:
            print(f"\n  ... processed {i}/{total} ({len(solo_users)} to remove so far)\n")

    # Summary
    print(f"\n{'='*80}")
    print(f"Storage project: {project_name} ({args.storage_project_uuid})")
    print(f"Total members checked: {total}")
    print(f"KEEP   (in other active projects): {len(multi_users)}")
    print(f"REMOVE (only in storage project):  {len(solo_users)}")
    print(f"Errors: {errors}")

    if solo_users:
        print(f"\n--- Users to remove ({len(solo_users)}) ---")
        for u in solo_users:
            n = u.get('name') or ''
            e = u.get('email') or ''
            b = u.get('bastion_login') or ''
            print(f"  {n:40s} {e:45s} {b}")

        # Write output CSV
        fields = ["uuid", "name", "email", "bastion_login"]
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for u in solo_users:
                writer.writerow(u)
        print(f"\nWrote {len(solo_users)} users to {args.output}")


if __name__ == "__main__":
    main()
