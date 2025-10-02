# Ceph Manager

## Code gen
```
openapi-generator validate -i openapi.yml
openapi-generator generate -i openapi.yml -g python-flask -o python-flask-server-generated
```