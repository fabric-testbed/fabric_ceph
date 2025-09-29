#!/bin/bash
# Swagger generate server stub based on specification, them merge it into the project.
# Use carefully! Commit always before using this script!
# The following structure is assumed:
# .
# +-- my_server
# |   +-- openapi_server
# User is expected to replace openapi_server with my_server/openapi_server after executing this script

# variables
STUB_DIR=python-flask-server-generated
WORKING_DIR=openapi_server
ARCHIVE_DIR=openapi_server_archive
SCRIPTS_DIR=$(pwd)
FILES_TO_COPY=(
  __init__.py
  __main__.py
)

DIRS_TO_COPY=(
  response
)

swagger-codegen generate -i openapi.json -l python-flask -o ${STUB_DIR}
find ${STUB_DIR}/openapi_server \( -type d -name .git -prune \) -o -type f -print0 | xargs -0 sed -i '' 's/openapi_server/fabric_ceph.openapi_server/g'


# check for STUB_DIR directory
if [ ! -d "$STUB_DIR" ]; then
    echo "[ERROR] Unable to find ${STUB_DIR}"
    exit 1
fi

# remove ARCHIVE_DIR and create new ARCHIVE_DIR from current WORKING_DIR
if [ -d "$ARCHIVE_DIR" ]; then
    rm -rf $ARCHIVE_DIR
fi
echo "[INFO] full copy of '${WORKING_DIR}' archived as '${ARCHIVE_DIR}'"
cp -r $WORKING_DIR $ARCHIVE_DIR

# create new WORKING_DIR
if [ -d "$WORKING_DIR" ]; then
    rm -rf $WORKING_DIR
fi
echo "[INFO] create new '${WORKING_DIR}' from '${STUB_DIR}'"
cp -r $STUB_DIR/openapi_server $WORKING_DIR

# copy relevant directories from ARCHIVE_DIR to new WORKING_DIR
for f in "${DIRS_TO_COPY[@]}"; do
    echo "[INFO] copy directory: ${f} to new ${WORKING_DIR}"
    cp -r $ARCHIVE_DIR/${f} $WORKING_DIR/${f}
done

# copy relevant files from ARCHIVE_DIR to new WORKING_DIR
for f in "${FILES_TO_COPY[@]}"; do
    echo "[INFO] copy file: ${f} to new ${WORKING_DIR}"
    cp $ARCHIVE_DIR/${f} $WORKING_DIR/${f}
done

# update controllers
echo "[INFO] update controllers to include response import"
while read f; do
    echo "---------------------------------------------------"
    echo "[INFO] updating file: ${f}"
    sed -i '' "/from openapi_server import util/a from openapi_server.response_code import ${f%???} as rc" \
        $WORKING_DIR/controllers/${f}
    sed -i '' "s/from fabric_cf.orchestrator.openapi_server import util/from fabric.openapi_server import util\\"$'\n'\\"from fabric.openapi_server.response import ${f%???} as rc/g" \
    $WORKING_DIR/controllers/${f}
    while read line; do
        if [[ $line == def* ]]; then
            echo "  - ${line}"
            func_name=$(echo $line | cut -d ':' -f 1 | cut -d ' ' -f 2-)
            echo "    ${func_name//=None/}"
            sed -i '' "0,/'do some magic!'/s//rc.${func_name//=None/}/" $WORKING_DIR/controllers/${f}
        fi
    done < <(cat $WORKING_DIR/controllers/${f})
done < <(ls -1 $WORKING_DIR/controllers | grep -v '^__*')

# completed
echo "[INFO] completed - check files prior to use"

# return to scripts directory and exit
cd $SCRIPTS_DIR || exit 0

