#!/bin/sh
set -e

# replace '-' with '_'
FORMATTED_TENANT_ID=$(echo "$TENANT_ID" | tr '-' '_')

# run dbt
dbt run --profiles-dir . --target dev --vars "{TENANT_ID: $FORMATTED_TENANT_ID, PROJECT_ID: $PROJECT_ID}"

# Optionally, print a success message
echo "dbt run completed successfully."

# Exit the script
exit 0