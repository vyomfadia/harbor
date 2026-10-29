#!/bin/bash
set -x

DIR="$(cd "$(dirname "$0")" && pwd)"
E2E_IMAGE="goharbor/harbor-e2e-engine:latest-api"

set +e

# optional Docker Hub credentials for the engine's image pulls
docker_cfg=()
if [ -n "${DOCKER_USER}" ]; then
    set +x
    cfg_dir=$(mktemp -d)
    printf '{"auths":{"https://index.docker.io/v1/":{"auth":"%s"}}}' "$(printf '%s:%s' "${DOCKER_USER}" "${DOCKER_PWD}" | base64 -w0)" > $cfg_dir/config.json
    docker_cfg=(-v $cfg_dir:/root/.docker)
    set -x
fi

docker ps
# run db auth api cases
if [ "$1" = 'DB' ]; then
    docker run -i --privileged \
        -v $DIR/../../:/drone -v $DIR/../:/ca "${docker_cfg[@]}" -w /drone $E2E_IMAGE \
        robot --exclude proxy_cache_*  --exclude replic_dockerhub \
        -v DOCKER_USER:"${DOCKER_USER}" -v DOCKER_PWD:${DOCKER_PWD} -v ip:$2 -v ip1: \
        -v http_get_ca:false -v HARBOR_PASSWORD:${HARBOR_ADMIN_PASSWD} -v HARBOR_ADMIN:${HARBOR_ADMIN} \
        /drone/tests/robot-cases/Group1-Nightly/Setup.robot \
        /drone/tests/robot-cases/Group0-BAT/API_DB.robot
elif [ "$1" = 'PROXY_CACHE' ]; then

    PROXY_CACHE_TESTCASE_TAG="proxy_cache_from_harbor"
    docker run -i --privileged \
        -v $DIR/../../:/drone -v $DIR/../:/ca "${docker_cfg[@]}" -w /drone $E2E_IMAGE \
        robot --include setup --include "${PROXY_CACHE_TESTCASE_TAG}" \
        -v DOCKER_USER:"${DOCKER_USER}" -v DOCKER_PWD:${DOCKER_PWD} -v ip:$2 -v ip1: \
        -v http_get_ca:false -v HARBOR_PASSWORD:${HARBOR_ADMIN_PASSWD} -v HARBOR_ADMIN:${HARBOR_ADMIN} \
        /drone/tests/robot-cases/Group1-Nightly/Setup.robot \
        /drone/tests/robot-cases/Group0-BAT/API_DB.robot
elif [ "$1" = 'LDAP' ]; then
    # run ldap api cases
    python $DIR/../../tests/configharbor.py \
        -H $IP -u $HARBOR_ADMIN -p $HARBOR_ADMIN_PASSWD \
        -c auth_mode=ldap_auth \
        ldap_url=ldap://$IP \
        ldap_search_dn=cn=admin,dc=example,dc=com \
        ldap_search_password=admin \
        ldap_base_dn=dc=example,dc=com \
        ldap_uid=cn
    docker run -i --privileged \
        -v $DIR/../../:/drone -v $DIR/../:/ca "${docker_cfg[@]}" -w /drone $E2E_IMAGE \
        robot \
        -v DOCKER_USER:"${DOCKER_USER}" -v DOCKER_PWD:${DOCKER_PWD} -v ip:$2 -v ip1: \
        -v http_get_ca:false -v HARBOR_PASSWORD:${HARBOR_ADMIN_PASSWD} -v HARBOR_ADMIN:${HARBOR_ADMIN} \
        /drone/tests/robot-cases/Group1-Nightly/Setup.robot \
        /drone/tests/robot-cases/Group0-BAT/API_LDAP.robot
else
    rc=999
fi
rc=$?
## --------------------------------------------- Package Harbor CI Logs -------------------------------------------
outfile="integration_logs.tar.gz"
sudo tar -zcvf $outfile output.xml log.html /var/log/harbor/*
pwd
ls -lh $outfile
exit $rc
