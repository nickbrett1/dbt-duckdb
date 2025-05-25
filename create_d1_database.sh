#!/usr/bin/env bash
set -euo pipefail

DATABASE_NAME="wdi"

echo "Checking if Cloudflare D1 database '$DATABASE_NAME' exists..."
# Look for a line in the output that contains "│ wdi " (using the box drawing vertical bar)
if npx wrangler d1 list | grep -q "│ ${DATABASE_NAME} "; then
    echo "Database '$DATABASE_NAME' already exists."
else
    echo "Creating Cloudflare D1 database '$DATABASE_NAME'..."
    npx wrangler d1 create "$DATABASE_NAME"
    echo "Database '$DATABASE_NAME' created successfully."
fi