#!/usr/bin/env bash
set -euo pipefail

resource_group="$(azd env get-value AZURE_RESOURCE_GROUP)"
container_app_name="$(azd env get-value CONTAINER_APP_NAME)"
key_vault_name="$(azd env get-value KEY_VAULT_NAME)"

fqdn="$(az containerapp show \
  --resource-group "${resource_group}" \
  --name "${container_app_name}" \
  --query properties.configuration.ingress.fqdn \
  --output tsv)"

run_key="$(az keyvault secret show \
  --vault-name "${key_vault_name}" \
  --name benchmark-api-key \
  --query value \
  --output tsv)"

curl --fail --silent --show-error \
  --request POST \
  --header "X-Run-Key: ${run_key}" \
  "https://${fqdn}/run"
