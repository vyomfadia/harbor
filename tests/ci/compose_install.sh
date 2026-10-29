#!/bin/bash
# Installs Harbor on this host with the docker-compose installer like api_common_install.sh, but from upstream's published
# goharbor/* images matching VERSION except for the components built by images.yml, which come from $LOCAL_REPO/<image>:$LOCAL_TAG.
# usage: compose_install.sh <ip>
set -euo pipefail

cd "$(dirname "$0")/../.."
ip=${1:?usage: $0 <ip>}
: "${LOCAL_REPO:=harbor-ci}" "${LOCAL_TAG:=ci}" "${METRIC_PORT:=9090}"
local_images='harbor-core|harbor-jobservice|harbor-portal|harbor-registryctl|registry-photon'

# the release, else its latest rc, else <VERSION>-dev, else dev
upstream_tag() {
  local v tags re p t
  v=$(cat VERSION)
  re=${v//./\\.}
  tags=$(curl -fsSL "https://hub.docker.com/v2/repositories/goharbor/prepare/tags?name=$v&page_size=100" | jq -r '.results[].name')
  for p in "^$re\$" "^$re-rc[0-9]+\$" "^$re-dev\$"; do
    t=$(grep -E "$p" <<<"$tags" | sort -V | tail -n1 || true)
    if [ -n "$t" ]; then echo "$t"; return; fi
  done
  echo dev
}
tag=${UPSTREAM_TAG:-$(upstream_tag)}
echo "upstream images: goharbor/*:$tag, local: $LOCAL_REPO/{$local_images}:$LOCAL_TAG"

sudo mkdir -p /data
sudo ./tests/generateCerts.sh "$ip"
sudo ./tests/hostcfg.sh
sudo sed -i "s/^hostname: .*/hostname: $ip/; s/^  port: 9090/  port: $METRIC_PORT/" make/harbor.yml
if [ -n "${GITHUB_TOKEN:-}" ]; then
  sudo sed -i "s/# github_token: xxx/github_token: $GITHUB_TOKEN/" make/harbor.yml
fi

docker run --rm -v /:/hostfs:z "goharbor/prepare:$tag" gencert -p /etc/harbor/tls/internal
sed "s#goharbor/prepare:dev#goharbor/prepare:$tag#" make/prepare | sudo tee make/prepare.ci >/dev/null
sudo bash make/prepare.ci --with-trivy
sudo rm -f make/prepare.ci

# debugging env from api_common_install.sh
echo "GC_TIME_WINDOW_HOURS=0" | sudo tee -a make/common/config/core/env
echo "EXECUTION_STATUS_REFRESH_INTERVAL_SECONDS=5" | sudo tee -a make/common/config/core/env
printf '\nHARBOR_EXPORTER_CACHE_TIME=0\n' | sudo tee -a make/common/config/exporter/env

# prepare pins its build number as the tag, which upstream does not publish for dev builds
sudo sed -E -i "s#image: goharbor/([a-z-]+):.*#image: goharbor/\1:$tag#; s#image: goharbor/($local_images):.*#image: $LOCAL_REPO/\1:$LOCAL_TAG#" make/docker-compose.yml
grep 'image:' make/docker-compose.yml
sudo docker compose -f make/docker-compose.yml up -d

for _ in $(seq 60); do
  if curl -ksfL "https://$ip/api/v2.0/systeminfo"; then echo; exit 0; fi
  sleep 5
done
sudo docker compose -f make/docker-compose.yml ps
echo "harbor did not become ready" >&2
exit 1
