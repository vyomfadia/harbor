#!/bin/bash
# Backend unit tests with only Postgres and Redis on localhost, run `make gen_apis` first.
# Unlike the upstream setup (ut_install.sh + ut_run.sh) there is no LDAP server, see SKIP_PKGS.
set -euo pipefail

cd "$(dirname "$0")/../.."
root=$PWD

: "${POSTGRESQL_HOST:=localhost}" "${POSTGRESQL_PORT:=5432}" "${POSTGRESQL_USR:=postgres}"
: "${POSTGRESQL_PWD:=root123}" "${POSTGRESQL_DATABASE:=registry}"
: "${HARBOR_ADMIN:=admin}" "${HARBOR_ADMIN_PASSWD:=Harbor12345}" "${CORE_SECRET:=tempString}"
: "${REDIS_HOST:=localhost}" "${UTTEST:=true}"
export POSTGRESQL_HOST POSTGRESQL_PORT POSTGRESQL_USR POSTGRESQL_PWD POSTGRESQL_DATABASE
export HARBOR_ADMIN HARBOR_ADMIN_PASSWD CORE_SECRET REDIS_HOST UTTEST

export POSTGRES_MIGRATION_SCRIPTS_PATH="$root/make/migrations/postgresql/"
export TOKEN_PRIVATE_KEY_PATH="$root/tests/private_key.pem"

# 16-byte key for lib/encrypt
keydir=$(mktemp -d)
printf 'ci-secretkey-016' > "$keydir/secretkey"
export KEY_PATH="$keydir/secretkey"

# beego config for the core/api and core/controllers tests
mkdir -p src/core/conf
printf 'appname = Harbor\nrunmode = prod\nenablegzip = true\n\n[prod]\nhttpport = 8080\n' > src/core/conf/app.conf

# controller/systeminfo expects the root CA here
if [ ! -f /etc/core/ca/ca.crt ]; then
  sudo mkdir -p /etc/core/ca
  sudo cp tests/ca.crt /etc/core/ca/ca.crt
fi

# need an LDAP server
SKIP_PKGS=(
  src/controller/ldap$
  src/controller/usergroup/test$
  src/core/auth/ldap$
  src/pkg/ldap$
)
skip_re=$(IFS='|'; echo "${SKIP_PKGS[*]}")

cd src
mapfile -t pkgs < <(go list ./... | grep -v -E 'tests|testing' | grep -v -E "$skip_re")
echo "skipping: ${SKIP_PKGS[*]}"

# -p 1: the packages share one database
go test -count=1 -p 1 "$@" "${pkgs[@]}"
