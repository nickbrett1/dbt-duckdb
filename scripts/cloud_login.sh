#!/bin/bash
set -e

# Determine the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The project root directory is one level up from the scripts directory
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Change to the project root directory so that relative paths work correctly
cd "$PROJECT_ROOT"

# Doppler login. This is the critical path: the container's own agent reads its
# bearer/registry tokens from Doppler at start time (see scripts/agent-dev.sh),
# and without a login it comes up with no card.
if command -v doppler &> /dev/null; then
  if doppler whoami &> /dev/null 2>&1; then
    echo "OK: Already logged in to Doppler."
  else
    echo "INFO: Logging into Doppler (browser flow)..."
    echo "      If a browser does not open, copy the URL and auth code printed above"
    echo "      into your browser to complete the login, then return here."
    if doppler login --no-check-version --yes; then
      echo "OK: Doppler login successful."
    else
      echo "WARN: doppler login failed. The container agent fetches its secrets"
      echo "      from Doppler, so it will not register until you are logged in."
    fi
  fi
else
  echo "WARN: doppler is not installed - build the container first, then re-run."
fi

# Tailscale login. Joining the tailnet is what gives the agent a non-loopback
# card URL and what makes the container reachable by the LiteLLM proxy, which
# dials the card. --hostname sets the name it joins under, and that name is what
# the agent advertises, so it is the repo name.
if command -v tailscale &> /dev/null; then
  if ! pgrep -x tailscaled > /dev/null; then
    echo "INFO: Starting Tailscale daemon..."
    sudo start-stop-daemon --start --background --oknodo --pidfile /var/run/tailscaled.pid --make-pidfile --exec /usr/sbin/tailscaled -- --state=/var/lib/tailscale/tailscaled.state
    sleep 2
  fi
  if ! sudo tailscale status &> /dev/null; then
    echo "INFO: Logging into Tailscale..."
    sudo tailscale up --hostname=dbt-duckdb
  else
    echo "OK: Already logged in to Tailscale."
  fi
else
  echo "WARN: tailscale is not installed. Build the container first - post-create"
  echo "      installs it - then re-run this script."
fi

echo "Cloud login script finished."
