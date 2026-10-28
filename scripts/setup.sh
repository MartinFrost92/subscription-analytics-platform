#!/usr/bin/env bash
set -euo pipefail

die() { printf 'Setup failed: %s\n' "$*" >&2; exit 1; }
if command -v gcloud >/dev/null 2>&1; then
    gcloud_cli=gcloud
elif command -v gcloud.cmd >/dev/null 2>&1; then
    gcloud_cli=gcloud.cmd
else
    die 'Install Google Cloud CLI first: https://cloud.google.com/sdk/docs/install (Windows: winget install --id Google.CloudSDK --exact). Reopen your terminal afterward.'
fi
"$gcloud_cli" --version >/dev/null 2>&1 || die 'Google Cloud CLI could not start. Repair its installation, including bundled Python, and reopen your terminal.'
printf 'Checking Google Application Default Credentials...\n'
"$gcloud_cli" auth application-default print-access-token >/dev/null || die "Google authentication is missing or invalid. Run: $gcloud_cli auth application-default login, then rerun setup."
gcloud_config_dir=$("$gcloud_cli" info --format='value(config.paths.global_config_dir)')
gcloud_config_dir=${gcloud_config_dir//$'\r'/}
# Windows gcloud returns a Windows path even when called from Git Bash or WSL.
if command -v cygpath >/dev/null 2>&1; then
    gcloud_config_dir=$(cygpath -u "$gcloud_config_dir")
elif [[ "$gcloud_config_dir" =~ ^[A-Za-z]: ]] && command -v wslpath >/dev/null 2>&1; then
    gcloud_config_dir=$(wslpath -u "$gcloud_config_dir")
fi
adc_path="$gcloud_config_dir/application_default_credentials.json"
[[ -f "$adc_path" ]] || die "Google ADC file was not found. Run: $gcloud_cli auth application-default login, then rerun setup."

command -v docker >/dev/null 2>&1 || die 'Install Docker with Docker Compose v2 first.'
docker compose version >/dev/null 2>&1 || die 'Docker Compose v2 is required.'
docker info >/dev/null 2>&1 || die 'Start Docker (Linux containers) and check docker info.'
[[ $(docker info --format '{{.OSType}}') == linux ]] || die 'Switch Docker to Linux containers.'
docker compose up --help | grep -q -- '--wait-timeout' || die 'Update Docker Compose to v2.14 or newer.'

cd "$(dirname "${BASH_SOURCE[0]}")/../container"
# Keep Git Bash from converting container paths to Windows paths.
export MSYS_NO_PATHCONV=1
mkdir -p secrets
if [[ ! -f secrets/google-adc.json ]] || ! [[ "$adc_path" -ef secrets/google-adc.json ]]; then
    cp "$adc_path" secrets/google-adc.json
fi
compose=(docker compose -f compose.yaml -f compose.bigquery.yaml)

if [[ ! -e .env ]]; then
    printf 'Creating container/.env with random local secrets...\n'
    # The existing runtime image avoids requiring host Python or OpenSSL.
    env_content=$(docker run --rm -i --entrypoint python apache/airflow:3.1.7-python3.12 -c '
import base64, secrets, sys
values = {key: secrets.token_hex(32) for key in (
    "AIRFLOW_DB_PASSWORD", "ANALYTICS_DB_PASSWORD",
    "AIRFLOW_JWT_SECRET", "AIRFLOW_API_SECRET_KEY", "SOURCE_API_KEY")}
values["AIRFLOW_FERNET_KEY"] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
for line in sys.stdin.read().splitlines():
    key = line.partition("=")[0]
    print(f"{key}={values[key]}" if key in values else line)
' < .env.example)
    # Also refuse to overwrite a file created concurrently.
    (umask 077; set -o noclobber; printf '%s\n' "$env_content" > .env)
    unset env_content
else
    printf 'Preserving existing container/.env.\n'
    # Repair older setups without rotating existing credentials or API keys.
    env_content=$(docker run --rm -i --entrypoint python apache/airflow:3.1.7-python3.12 -c '
import secrets, sys
lines = sys.stdin.read().splitlines()
found = False
for index, line in enumerate(lines):
    key, separator, value = line.partition("=")
    if key.strip() == "SOURCE_API_KEY" and separator:
        found = True
        if not value.strip().strip(chr(34)).strip(chr(39)):
            lines[index] = "SOURCE_API_KEY=" + secrets.token_hex(32)
if not found:
    lines.append("SOURCE_API_KEY=" + secrets.token_hex(32))
print("\n".join(lines))
' < .env)
    if [[ "$env_content" != "$(cat .env)" ]]; then
        printf 'Adding missing SOURCE_API_KEY to container/.env...\n'
        printf '%s\n' "$env_content" > .env
    fi
    unset env_content
fi

"${compose[@]}" config --quiet
printf 'Starting PostgreSQL...\n'
"${compose[@]}" up -d --build metadata-db analytics-db
# Older volumes may lack subscription_source, which its health check requires.
for service in metadata-db analytics-db; do
    ready=false
    for ((attempt=0; attempt<60; attempt++)); do
        if "${compose[@]}" exec -T "$service" sh -c 'pg_isready -U "$POSTGRES_USER" -d postgres' >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 2
    done
    [[ $ready == true ]] || die "$service did not become ready; inspect docker compose logs from container/."
done
"${compose[@]}" exec -T analytics-db sh -c 'exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres' <<'SQL'
SELECT format('CREATE DATABASE %I', name)
FROM (VALUES ('warehouse'), ('subscription_source')) AS required(name)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = name)
\gexec
SQL

printf 'Starting Airflow (Compose runs its idempotent metadata migration)...\n'
if ! "${compose[@]}" up -d --build --wait --wait-timeout 300; then
    "${compose[@]}" ps -a
    die 'Services failed to start. Inspect docker compose logs from container/; configuration and volumes were preserved.'
fi
"${compose[@]}" exec -T metadata-db sh -c 'exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d airflow -tAc "SELECT 1"' >/dev/null
"${compose[@]}" ps -a
if ! "${compose[@]}" exec -T airflow-scheduler python /opt/airflow/scripts/verify-bigquery.py; then
    die 'Google Cloud verification failed. Check ADC login, permissions and datasets using container/BIGQUERY.md, then rerun setup.'
fi
printf '\nSetup complete. Airflow address (no sign-in required):\n'
"${compose[@]}" port airflow-api-server 8080
