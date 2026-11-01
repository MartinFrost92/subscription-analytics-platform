#!/usr/bin/env bash
# Offline regression checks: no Google calls, containers, or real ADC changes.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/scripts" "$scratch/container" "$scratch/gcloud"
cp "$repo/scripts/setup.sh" "$scratch/scripts/setup.sh"
printf '{}\n' > "$scratch/gcloud/application_default_credentials.json"
printf 'SOURCE_API_KEY=test\n' > "$scratch/container/.env"
export scratch
gcloud() {
    printf '%s\n' "$*" >> "$scratch/calls"
    case "$*" in
        '--version') return 0 ;;
        'info '*) printf '%s\n' "$scratch/gcloud" ;;
        'auth application-default login '*) token_status=0; return "$login_status" ;;
        'auth application-default print-access-token') return "$token_status" ;;
        *) return 99 ;;
    esac
}
docker() {
    case "$*" in
        'compose version'|'info') return 0 ;;
        'info --format '*) printf 'linux\n' ;;
        'compose up --help') printf '%s\n' '--wait-timeout' ;;
        'run '*service_account_impersonation_url*) return "$metadata_status" ;;
        'run '*) printf 'SOURCE_API_KEY=test\n' ;;
        # End each successful auth scenario before any provisioning.
        'compose -f compose.yaml -f compose.bigquery.yaml config --quiet') return 42 ;;
        *) return 99 ;;
    esac
}
gcloud.cmd() { gcloud "$@"; }
export -f gcloud gcloud.cmd docker
for scenario in direct valid expired denied; do
    export metadata_status=1 token_status=0 login_status=0
    case "$scenario" in
        valid) metadata_status=0 ;;
        expired) metadata_status=0; token_status=1 ;;
        denied) login_status=1 ;;
    esac
    : > "$scratch/calls"
    status=0
    bash "$scratch/scripts/setup.sh" > "$scratch/output" 2>&1 || status=$?
    if [[ "$scenario" == denied ]]; then
        [[ "$status" == 1 ]]
        grep -q 'roles/iam.serviceAccountTokenCreator' "$scratch/output"
    else
        [[ "$status" == 42 ]]
    fi
    if [[ "$scenario" == valid ]]; then
        ! grep -q 'application-default login' "$scratch/calls"
    else
        grep -q 'application-default login --impersonate-service-account=airflow-writer@healthy-pager-398915.iam.gserviceaccount.com --disable-quota-project' "$scratch/calls"
    fi
    printf 'PASS %s ADC\n' "$scenario"
done
