#!/bin/bash
# This file is executed every time the dev container starts up or resumes.
#
# This container runs its own a2a-goose agent - the `container-agent` capability
# (https://github.com/nickbrett1/genproj/blob/main/specs/008-genproj-container-agent/spec.md).
# The agent registers in the hub under the name "dbt-duckdb-dev"; scripts/agent-dev.sh is
# app-owned, so regenerating this project never overwrites it.

echo "INFO: Checking Tailscale status..."
if ! pgrep -x tailscaled >/dev/null; then
    echo "INFO: Tailscale daemon not running. Starting it..."
    sudo start-stop-daemon --start --background --oknodo --pidfile /var/run/tailscaled.pid --make-pidfile --exec /usr/sbin/tailscaled -- --state=/var/lib/tailscale/tailscaled.state
fi

echo "INFO: Checking the container agent..."
if [ -x "/workspaces/dbt-duckdb/scripts/agent-dev.sh" ]; then
    "/workspaces/dbt-duckdb/scripts/agent-dev.sh" start || true
else
    echo "WARN: scripts/agent-dev.sh not found, skipping the container agent"
fi

echo "INFO: Services check/startup complete."
