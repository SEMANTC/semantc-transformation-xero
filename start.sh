#!/bin/sh
set -e

# replace '-' with '_'
FORMATTED_USER_ID=$(echo "$USER_ID" | tr '-' '_')

# run dbt
dbt run --profiles-dir . --target dev --vars "{USER_ID: $FORMATTED_USER_ID, PROJECT_ID: $PROJECT_ID}"

# Optionally, print a success message
echo "dbt run completed successfully."

# Exit the script
exit 0