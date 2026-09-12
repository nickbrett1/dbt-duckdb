#!/usr/bin/env bash
#
# Everything the data pipeline needs beyond Python, installed once per step.
# CircleCI installed these inline in every job; a shared script keeps the three
# steps readable and identical.
#
# rclone is how the pipeline reads and writes Cloudflare R2; node is there because
# update_d1.py drives wrangler; the Doppler CLI injects secrets at runtime (the
# repo's own npm scripts already use `doppler run`), which is how this pipeline
# replaces CircleCI's "context: common".
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

missing() { ! command -v "$1" >/dev/null 2>&1; }

if missing curl || missing unzip || missing sqlite3 || missing jq; then
	apt-get update
	apt-get install -y --no-install-recommends curl unzip ca-certificates sqlite3 jq
fi

if missing rclone; then
	curl -fsSL https://rclone.org/install.sh | bash
fi

if missing node; then
	curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
	apt-get install -y --no-install-recommends nodejs
fi

if missing doppler; then
	curl -Ls --tlsv1.2 --proto "=https" --retry 3 https://cli.doppler.com/install.sh | sh
fi

python -m pip install --no-cache-dir -r requirements.txt
