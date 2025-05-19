#!/usr/bin/env bash
# filepath: /workspaces/dbt-duckdb/create_r2_bucket.sh

set -euo pipefail

BUCKET_NAME="wdi"

echo "Checking if Cloudflare R2 bucket '$BUCKET_NAME' exists..."
if npx wrangler r2 bucket list | grep -qE "^name:[[:space:]]*${BUCKET_NAME}\b"; then
    echo "Bucket '$BUCKET_NAME' already exists."
else
    echo "Creating Cloudflare R2 bucket '$BUCKET_NAME'..."
    npx wrangler r2 bucket create "$BUCKET_NAME"
    echo "Bucket '$BUCKET_NAME' created successfully."
fi