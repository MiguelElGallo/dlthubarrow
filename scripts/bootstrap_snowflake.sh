#!/usr/bin/env bash
set -euo pipefail

connection="${SNOW_CONNECTION:-mpz}"
benchmark_role="${BENCHMARK_ROLE:-AZAPP_ROLE}"
benchmark_user="${BENCHMARK_USER:-AZAPP}"
benchmark_database="${BENCHMARK_DATABASE:-dummy}"
benchmark_warehouse="${BENCHMARK_WAREHOUSE:-COMPUTE_WH}"
pat_name="${PAT_NAME:-AZAPP_PAT}"
days_to_expiry="${DAYS_TO_EXPIRY:-30}"

render_template() {
  local template_path="$1"
  local output_path="$2"
  sed \
    -e "s|{{ BENCHMARK_ROLE }}|${benchmark_role}|g" \
    -e "s|{{ BENCHMARK_USER }}|${benchmark_user}|g" \
    -e "s|{{ BENCHMARK_DATABASE }}|${benchmark_database}|g" \
    -e "s|{{ BENCHMARK_WAREHOUSE }}|${benchmark_warehouse}|g" \
    -e "s|{{ PAT_NAME }}|${pat_name}|g" \
    -e "s|{{ DAYS_TO_EXPIRY }}|${days_to_expiry}|g" \
    "${template_path}" > "${output_path}"
}

bootstrap_sql="$(mktemp)"
pat_sql="$(mktemp)"
trap 'rm -f "${bootstrap_sql}" "${pat_sql}"' EXIT

render_template scripts/bootstrap_snowflake.sql "${bootstrap_sql}"
render_template scripts/create_pat.sql "${pat_sql}"

snow sql -c "${connection}" -f "${bootstrap_sql}"

echo
echo "Creating PAT. Copy the token_secret from the output and store it in Azure Key Vault."
current_user="$(
  snow sql -c "${connection}" -q "select current_user();" --format CSV --silent \
    | tail -n 1
)"
snow sql -c "${connection}" -q "GRANT ROLE ${benchmark_role} TO USER \"${current_user}\";"
snow sql -c "${connection}" --role "${benchmark_role}" -f "${pat_sql}"
