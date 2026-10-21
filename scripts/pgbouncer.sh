#!/usr/bin/env bash
# The three pgbouncers the test suite runs through (`tests/test_pgbouncer.py`,
# `tests/test_migrations.py`), in front of the test postgres on 127.0.0.1:5432:
#
#     port 6431  pgbouncer 1.20.1, transaction pooling, no prepared-statement support
#     port 6432  pgbouncer 1.22.1, transaction pooling, max_prepared_statements = 100
#     port 6433  pgbouncer 1.22.1, session pooling
#
#     scripts/pgbouncer.sh up      # or: just pgbouncer up
#     scripts/pgbouncer.sh down
#
# The same images and settings as the CI services in .github/workflows/ci.yml;
# change both together. `server_round_robin = 1` hands each transaction the
# server connection idle longest, so a test that spreads the pool over several
# servers (`conftest.pgbouncer`) lands consecutive transactions on different
# ones — without it, one idle client keeps getting the same server and every
# cross-server hazard stays hidden.
set -euo pipefail

usage() { echo "usage: $0 {up|down}" >&2; exit 2; }
[[ $# -eq 1 ]] || usage

POOLERS=(
    "rowform-pgbouncer-1.20-transaction 6431 1.20.1-p0 transaction 0"
    "rowform-pgbouncer-1.22-transaction 6432 1.22.1-p0 transaction 100"
    "rowform-pgbouncer-1.22-session 6433 1.22.1-p0 session 0"
)

case "$1" in
up)
    for pooler in "${POOLERS[@]}"; do
        read -r name port tag mode prepared <<<"$pooler"
        docker rm -f "$name" >/dev/null 2>&1 || true
        docker run -d --name "$name" --network host \
            -e LISTEN_PORT="$port" \
            -e DB_HOST=127.0.0.1 -e DB_PORT=5432 -e DB_NAME=rowform_bench \
            -e DB_USER=postgres -e DB_PASSWORD=postgres \
            -e AUTH_TYPE=scram-sha-256 -e ADMIN_USERS=postgres \
            -e POOL_MODE="$mode" -e MAX_PREPARED_STATEMENTS="$prepared" \
            -e DEFAULT_POOL_SIZE=4 -e SERVER_ROUND_ROBIN=1 \
            "edoburu/pgbouncer:$tag" >/dev/null
        echo "$name on 127.0.0.1:$port"
    done
    ;;
down)
    for pooler in "${POOLERS[@]}"; do
        read -r name _ <<<"$pooler"
        docker rm -f "$name" >/dev/null 2>&1 || true
    done
    ;;
*) usage ;;
esac
