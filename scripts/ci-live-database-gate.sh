#!/bin/sh
set -eu

# This runner is intentionally narrower than the Windows operator gate. It is
# for a fresh, disposable GitHub-hosted runner only and executes no vendor lane.
umask 077

TIMESCALE_IMAGE="timescale/timescaledb:2.28.2-pg17@sha256:909e2c7577c074517c8936b6d8799294b33f25fb5bad978a2199250665f1ce1a"
CONTAINER_CREATED=0
CONTAINER_NAME=""

refuse() {
    printf 'live database CI gate refused: %s\n' "$1" >&2
    exit 1
}

cleanup() {
    status=$?
    trap - EXIT INT TERM

    if [ "$CONTAINER_CREATED" -eq 1 ]; then
        if [ "$status" -ne 0 ]; then
            docker logs --tail 200 "$CONTAINER_NAME" 2>&1 || true
        fi
        docker rm --force --volumes "$CONTAINER_NAME" >/dev/null 2>&1 || true
    fi

    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

[ "${GITHUB_ACTIONS:-}" = "true" ] || refuse "GITHUB_ACTIONS must be true"
[ "${RUNNER_ENVIRONMENT:-}" = "github-hosted" ] || refuse "runner must be GitHub-hosted"
[ "${RUNNER_OS:-}" = "Linux" ] || refuse "runner OS must be Linux"
[ "${RUNNER_ARCH:-}" = "X64" ] || refuse "runner architecture must be X64"
[ -n "${GITHUB_WORKSPACE:-}" ] || refuse "GITHUB_WORKSPACE must be set"
[ -n "${RUNNER_TEMP:-}" ] || refuse "RUNNER_TEMP must be set"
[ -n "${GITHUB_RUN_ID:-}" ] || refuse "GITHUB_RUN_ID must be set"
[ -n "${GITHUB_RUN_ATTEMPT:-}" ] || refuse "GITHUB_RUN_ATTEMPT must be set"

command -v git >/dev/null 2>&1 || refuse "git is unavailable"
command -v docker >/dev/null 2>&1 || refuse "Docker is unavailable"
command -v openssl >/dev/null 2>&1 || refuse "OpenSSL is unavailable"
command -v uv >/dev/null 2>&1 || refuse "uv is unavailable"

workspace=$(CDPATH='' cd -- "$GITHUB_WORKSPACE" && pwd -P)
repository=$(git rev-parse --show-toplevel 2>/dev/null) || refuse "not in a Git checkout"
repository=$(CDPATH='' cd -- "$repository" && pwd -P)
current=$(pwd -P)
[ "$repository" = "$workspace" ] || refuse "Git root must equal GITHUB_WORKSPACE"
[ "$current" = "$workspace" ] || refuse "run from the repository root"
[ ! -e "$workspace/.env" ] || refuse ".env must not exist in the CI checkout"
[ ! -e "$workspace/data/pgdata" ] || refuse "a reusable Postgres data directory exists"
[ ! -e "$RUNNER_TEMP/stockapi-live-postgres" ] || refuse "test scratch directory already exists"
[ -x "$workspace/scripts/db-init/02-runtime-role.sh" ] || refuse "role bootstrap is not executable"

[ "$(docker info --format '{{.OSType}}' 2>/dev/null)" = "linux" ] ||
    refuse "Docker must use a Linux engine"

case "${AUTOMATION_ENABLED:-false}" in
    false | False | FALSE | 0 | no | off) ;;
    *) refuse "AUTOMATION_ENABLED must remain false" ;;
esac

if env | grep -Eq '^(POLYGON_API_KEY|FMP_API_KEY|FINNHUB_API_KEY|NASDAQ_DATA_LINK_API_KEY|ALPACA_API_KEY|ALPACA_API_SECRET|DATABENTO_API_KEY)=.+'; then
    refuse "vendor credentials must be absent"
fi

export AUTOMATION_ENABLED=false
export POLYGON_TOTAL_CALL_BUDGET=0
unset POLYGON_API_KEY FMP_API_KEY FINNHUB_API_KEY NASDAQ_DATA_LINK_API_KEY
unset ALPACA_API_KEY ALPACA_API_SECRET DATABENTO_API_KEY
unset TEST_RATE_LIMIT_REDIS_URL TEST_REDIS_URL
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY
unset http_proxy https_proxy all_proxy no_proxy

owner_password=$(openssl rand -hex 24)
app_password=$(openssl rand -hex 24)
builder_password=$(openssl rand -hex 24)
[ "$owner_password" != "$app_password" ] || refuse "generated owner/runtime passwords collide"
[ "$owner_password" != "$builder_password" ] || refuse "generated owner/builder passwords collide"
[ "$app_password" != "$builder_password" ] || refuse "generated runtime/builder passwords collide"

printf '::add-mask::%s\n' "$owner_password"
printf '::add-mask::%s\n' "$app_password"
printf '::add-mask::%s\n' "$builder_password"

CONTAINER_NAME="stockapi-ci-timescaledb-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
case "$CONTAINER_NAME" in
    *[!A-Za-z0-9_.-]*) refuse "derived container name is invalid" ;;
esac
if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
    refuse "throwaway container name already exists"
fi

if ! docker run --detach \
    --name "$CONTAINER_NAME" \
    --platform linux/amd64 \
    --pull always \
    --publish 127.0.0.1:5432:5432 \
    --shm-size 256m \
    --volume /var/lib/postgresql/data \
    --mount "type=bind,source=$workspace/scripts/db-init,target=/docker-entrypoint-initdb.d,readonly" \
    --env POSTGRES_USER=stockapi_owner \
    --env POSTGRES_PASSWORD="$owner_password" \
    --env POSTGRES_DB=stockapi_test \
    --env POSTGRES_APP_PASSWORD="$app_password" \
    --env POSTGRES_SNAPSHOT_BUILDER_PASSWORD="$builder_password" \
    --env TIMESCALEDB_TELEMETRY=off \
    --health-cmd "pg_isready -h 127.0.0.1 -U stockapi_owner -d stockapi_test" \
    --health-interval 3s \
    --health-timeout 5s \
    --health-retries 100 \
    "$TIMESCALE_IMAGE" >/dev/null; then
    docker rm --force --volumes "$CONTAINER_NAME" >/dev/null 2>&1 || true
    refuse "TimescaleDB container failed to start"
fi
CONTAINER_CREATED=1

attempt=0
while :; do
    state=$(docker inspect --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' "$CONTAINER_NAME" 2>/dev/null) ||
        refuse "TimescaleDB container disappeared"
    container_state=${state%% *}
    health=${state#* }

    case "$container_state" in
        exited | dead) refuse "TimescaleDB exited during initialization" ;;
    esac
    [ "$health" != "unhealthy" ] || refuse "TimescaleDB became unhealthy"

    if [ "$health" = "healthy" ] &&
        docker exec "$CONTAINER_NAME" \
            pg_isready -h 127.0.0.1 -U stockapi_owner -d stockapi_test \
            >/dev/null 2>&1; then
        break
    fi

    attempt=$((attempt + 1))
    [ "$attempt" -lt 100 ] || refuse "timed out waiting for TimescaleDB"
    sleep 3
done

[ "$(docker inspect --format '{{.Config.Image}}' "$CONTAINER_NAME")" = "$TIMESCALE_IMAGE" ] ||
    refuse "running image does not match the pinned digest"
[ "$(docker port "$CONTAINER_NAME" 5432/tcp)" = "127.0.0.1:5432" ] ||
    refuse "database is not bound only to the required loopback port"

bootstrap=$(
    docker exec "$CONTAINER_NAME" psql \
        --username stockapi_owner \
        --dbname stockapi_test \
        --tuples-only \
        --no-align \
        --command "SELECT (EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') AND (SELECT count(*) FROM pg_roles WHERE rolname IN ('stockapi_app', 'stockapi_snapshot_builder') AND rolcanlogin) = 2)::int"
)
[ "$bootstrap" = "1" ] || refuse "Timescale extension or runtime-role bootstrap is missing"

# Re-running the transactional bootstrap proves that a fresh CI database does
# not depend on a one-shot role mutation that fails on an existing role.
docker exec "$CONTAINER_NAME" \
    sh /docker-entrypoint-initdb.d/02-runtime-role.sh >/dev/null

export TEST_DATABASE_URL="postgresql+asyncpg://stockapi_owner:${owner_password}@127.0.0.1:5432/stockapi_test"
export TEST_RUNTIME_DATABASE_URL="postgresql+asyncpg://stockapi_app:${app_password}@127.0.0.1:5432/stockapi_test"
export TEST_SNAPSHOT_BUILDER_DATABASE_URL="postgresql+asyncpg://stockapi_snapshot_builder:${builder_password}@127.0.0.1:5432/stockapi_test"
export TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET=stockapi-test-only
export UV_OFFLINE=1

uv run --frozen --no-sync pytest \
    -p no:cacheprovider \
    --basetemp "$RUNNER_TEMP/stockapi-live-postgres" \
    tests/integration/test_bars_live_gate.py \
    -v
