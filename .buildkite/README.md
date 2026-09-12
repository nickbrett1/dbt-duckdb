# Buildkite

See `pipeline.yml` (the header explains the port from CircleCI). Secrets
are injected with `doppler run` using the agent's DOPPLER_TOKEN; the toolchain
install lives in `scripts/setup-toolchain.sh`.

Agent prerequisites: `plugins-path` in buildkite-agent.cfg, Docker, and a queue
named `mac-studio-linux`.
