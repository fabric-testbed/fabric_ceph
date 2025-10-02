# FABRIC Ceph Manager Service

A lightweight REST service that orchestrates **Ceph Dashboard APIs** across one or more Ceph clusters. It standardizes common workflows:

* **CephX users**: list, create/update (upsert), delete, export keyrings, and **synchronize the same keyring across clusters**.
* **CephFS subvolumes**: create subvolume groups, create/resize subvolumes, get paths, existence checks, and delete — **applied consistently across clusters**.
* **Multi-cluster logic**:

  * “**First success**” discovery APIs (e.g., list users, export keyrings, get subvolume info).
  * “**Ensure/sync**” APIs (create/update on a source cluster, then fan-out imports/operations to other clusters).

Built with **Flask/Connexion** (OpenAPI-first), `requests` for Dashboard calls, and `paramiko` to run `ceph auth import` over SSH when replicating CephX secrets.

---

## Table of Contents

* [Features](#features)
* [Architecture](#architecture)
* [OpenAPI](#openapi)
* [Code generation](#code-generation)
* [Configuration](#configuration)
* [Running locally](#running-locally)
* [Docker](#docker)
* [API usage (curl)](#api-usage-curl)
  * [CephX users](#cephx-users)
  * [CephFS subvolumes](#cephfs-subvolumes)
* [Multi-cluster behavior](#multi-cluster-behavior)
* [Troubleshooting](#troubleshooting)
* [Security notes](#security-notes)
* [License](#license)

---

## Features

* 🔐 **CephX user lifecycle** via Dashboard REST:

  * `GET /cluster/user` — list
  * `POST /cluster/user` — create
  * `PUT /cluster/user` — update (with upsert in helper)
  * `DELETE /cluster/user/{entity}` — delete
  * `POST /cluster/user/export` — export keyrings (JSON or plaintext)
* 📦 **Keyring fan-out**: export the keyring from a “source” cluster and **SSH import** it to all other clusters (`ceph auth import`) so the **same secret** is valid everywhere.
* 🗂 **CephFS subvolumes**:

  * `PUT /cephfs/subvolume/{vol_name}` — create or resize subvolume
  * `GET /cephfs/subvolume/{vol_name}/info` — info/getpath
  * `GET /cephfs/subvolume/{vol_name}/exists` — existence probe
  * `DELETE /cephfs/subvolume/{vol_name}` — delete
* 🧠 **Cross-cluster helpers** (in `cluster_helper.py`):

  * `ensure_user_across_clusters`, `update_user_across_clusters`, `delete_user_across_clusters`
  * `ensure_subvolume_across_clusters`, `delete_subvolume_across_clusters`
  * `list_users_first_success`, `export_users_first_success`
* 🧾 Clear **OpenAPI 3.0** contract (`openapi.yml`) and **Connexion** handlers.

---

## Architecture

```
               +-------------------+            +---------------------+
  Client  ---> |  Flask + Connexion|  REST ---> |  DashClient (REST)  |
               |  (OpenAPI-first)  |            |  /api/auth + calls  |
               +-------------------+            +----------+----------+
                         |                                |
                         |                                | Keyring text
                         |                            export_keyring()
                         v                                |
               +-------------------+                      |
               |  cluster_helper   | <-- SSH (paramiko) --+
               |  (multi-cluster   |     ceph auth import
               |   orchestration)  |
               +-------------------+
```

* **DashClient** logs into each cluster’s **Dashboard** (`/api/auth`) to get a JWT then calls the documented endpoints.
* For **keyring replication**, the service uses SSH to run `ceph auth import` on the target clusters so the secret stays identical.
* **Config-driven** multi-cluster: names, endpoints, creds, and SSH parameters per cluster.

---

## OpenAPI

* Spec: `fabric_ceph/openapi.yml`
* Served with Connexion; controller modules:

  * `fabric_ceph.openapi_server.controllers.cluster_user_controller`
  * `fabric_ceph.openapi_server.controllers.ceph_fs_controller`
  * `fabric_ceph.openapi_server.controllers.version_controller`

### Notable behaviors

* `X-Cluster` header (optional): influence which clusters are tried and in what order, e.g.
  `X-Cluster: europe,us-west`
* `application/json` everywhere.
* Errors are normalized using `status_4xx` / `status_5xx` schemas.

---
## Code generation

The OpenAPI spec for this service is maintained on SwaggerHub:  
https://app.swaggerhub.com/apis/RENCI3/ceph/1.0.0

Use the commands below to validate the spec and generate fresh Flask server stubs.  
The helper script will archive the previous scaffold to `openapi_server_archive/` and place the newly generated code in `openapi_server/` — you can then review and merge changes as needed.

> Prereqs: `openapi-generator` installed (e.g., `brew install openapi-generator`) and Java available on your PATH.

```bash
cd fabric_ceph

# Validate the spec
openapi-generator validate -i openapi.yml

# Generate a Python Flask server scaffold (into a temp folder)
openapi-generator generate -i openapi.yml -g python-flask -o python-flask-server-generated

# Archive the old server code and install the new one
./swagger_code_gen.sh
```
---
## Configuration

Example `config.yml`:

```yaml
cluster:
  europe:
    ceph_cli: ceph
    default_fs: CEPH-FS-01
    dashboard:
      endpoints: [ "https://10.145.126.2:8443" ]
      user: admin
      password: abcd1234
      ssh_user: rocky          # used for importing keyrings
      ssh_key: ~/.ssh/id_rsa_ceph
    rgw_admin:
      endpoints: [ "http://10.145.124.2:8080" ]
      admin_access_key: admin_ak
      admin_secret_key: admin_sk
      ssh_user: rocky
      ssh_key: ~/.ssh/id_rsa_ceph

runtime:
  service_project:
logging:
  log-directory: /var/log/actor
  log-file: actor.log
  metrics-log-file: metrics.log
  log-level: INFO
  log-retain: 5
  log-size: 5000000
  logger: ceph-mgr

oauth:
  jwks-url: https://cm.fabric-testbed.net/credmgr/certs
  key-refresh: 00:10:00
  verify-exp: true

core_api:
  enable: true
  host: https://uis.fabric-testbed.net
  token:
```

Environment overrides (per cluster, optional):

```
EUROPE_SSH_HOST, EUROPE_SSH_PORT, EUROPE_SSH_USER, EUROPE_SSH_KEY, EUROPE_SSH_PASSWORD
```

> Set `APP_CONFIG_PATH` to point the service at your YAML.

---

## Running locally

```bash
python -m venv .venv
. .venv/bin/activate
pip install --upgrade pip

# Install service
pip install -r requirements.txt
pip install -e .

# Run
export APP_CONFIG_PATH=/path/to/config.yml
python -m fabric_ceph
# or just: fabric_ceph
```

By default, the server binds to the host/port your app config or entrypoint sets (commonly 3500 are exposed in Docker; adjust to taste).

---

## Docker

**Dockerfile** is included. Make sure your build context includes `README.md`, `pyproject.toml`, and `fabric_ceph/`.

Build & run:

```bash
# From repo root
docker build -t fabric-ceph .

docker run --rm -p 3500:3500 \
  -e APP_CONFIG_PATH=/etc/fabric/ceph/config/config.yml \
  -v /local/config.yml:/etc/fabric/ceph/config/config.yml:ro \
  fabric-ceph
```

> The container starts `cron` (optional) and runs the module with the system Python (3.13). Ensure your requirements include `connexion`, `requests`, and `paramiko`.

---

## API usage (curl)

### Headers

```bash
# (If using bearer auth via Dashboard login)
-H "Authorization: Bearer <jwt>"
# Optional: influence cluster order
-H "X-Cluster: europe,lab"
```

### CephX users

List users:

```bash
curl -s http://localhost:3500/cluster/user
```

Create or update (upsert handled server-side):

```bash
curl -s -X POST http://localhost:3500/cluster/user \
  -H "Content-Type: application/json" \
  -d '{
        "user_entity": "client.demo",
        "capabilities": [
          {"entity": "mon", "cap": "allow r"},
          {"entity": "mds", "cap": "allow rw fsname=CEPH-FS-01 path=/volumes/_nogroup/demo"},
          {"entity": "osd", "cap": "allow rw tag cephfs data=CEPH-FS-01"},
          {"entity": "osd", "cap": "allow rw tag cephfs metadata=CEPH-FS-01"}
        ]
      }'
```

Update caps only:

```bash
curl -s -X PUT http://localhost:3500/cluster/user \
  -H "Content-Type: application/json" \
  -d '{"user_entity":"client.demo","capabilities":[{"entity":"mon","cap":"allow r"}]}'
```

Delete:

```bash
curl -s -X DELETE http://localhost:3500/cluster/user/client.demo
```

Export keyring(s):

```bash
curl -s -X POST http://localhost:3500/cluster/user/export \
  -H "Content-Type: application/json" \
  -d '{"entities":["client.demo","client.alice"]}'
```

> Internally, **cross-cluster sync** helpers can export the keyring from a source and **SSH-import** it to others, ensuring **identical secrets** everywhere.

### CephFS subvolumes

Create or resize subvolume:

```bash
# create with mode and quota
curl -s -X PUT http://localhost:3500/cephfs/subvolume/CEPH-FS-01 \
  -H "Content-Type: application/json" \
  -d '{"subvol_name":"alice","group_name":"fabric_staff","size":10737418240,"mode":"0777"}'

# resize quota
curl -s -X PUT http://localhost:3500/cephfs/subvolume/CEPH-FS-01 \
  -H "Content-Type: application/json" \
  -d '{"subvol_name":"alice","group_name":"fabric_staff","size":536870912}'
```

Get path/info:

```bash
curl -s "http://localhost:3500/cephfs/subvolume/CEPH-FS-01/info?subvol_name=alice&group_name=fabric_staff"
```

Exists:

```bash
curl -s "http://localhost:3500/cephfs/subvolume/CEPH-FS-01/exists?subvol_name=alice&group_name=fabric_staff"
```

Delete:

```bash
curl -s -X DELETE "http://localhost:3500/cephfs/subvolume/CEPH-FS-01?subvol_name=alice&group_name=fabric_staff"
```

---

## Multi-cluster behavior

* **First-success** queries:

  * `list_users_first_success` returns the first cluster that responds successfully.
  * `export_users_first_success` exports keyrings from the first cluster that can.
  * `get_subvolume_info`/`subvolume_exists` try clusters in `X-Cluster` order (or config order) and return on first success.
* **Ensure/sync** mutations:

  * `ensure_user_across_clusters` / `update_user_across_clusters`: pick a source (existing user or preferred cluster), apply caps (create if missing), **export keyring**, SSH **import** everywhere else.
  * `ensure_subvolume_across_clusters`: ensure group, create/resize subvolume on a source, then apply the same to all clusters; returns per-cluster paths.
  * `delete_*_across_clusters`: best-effort deletes with per-cluster results.

---

## Security notes

* Dashboard credentials grant cluster-admin control; protect `config.yml`.
* SSH key used for `ceph auth import` should be restricted and rotated.
* **Keyring fan-out** intentionally makes the **same secret** valid across clusters — treat the exported keyring as sensitive data.
* TLS verification: default behavior depends on your `DashClient` settings. Prefer HTTPS with CA validation in production.

---

## License

MIT © 2025 FABRIC Testbed

---

## Acknowledgements

* Ceph Dashboard REST API (for authentication and cluster/user/cephfs management).
* Thanks to contributors for testing multi-cluster synchronization and API shape validation.


## Code gen
Swagger API is maintained here: https://app.swaggerhub.com/apis/RENCI3/ceph/1.0.0

Generate the code using the following commands
this should place latest code in `openapi_server` and old code in `openapi_server_archive`
Merge as needed
```
cd fabric_ceph
openapi-generator validate -i openapi.yml
openapi-generator generate -i openapi.yml -g python-flask -o python-flask-server-generated
./swagger_code_gen.sh
```
