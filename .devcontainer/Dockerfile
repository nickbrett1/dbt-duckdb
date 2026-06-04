FROM mcr.microsoft.com/devcontainers/python:1-3.13-bookworm

# Remove the Yarn repository which has an expired GPG key, causing `apt-get update` to fail during feature installation
RUN rm -f /etc/apt/sources.list.d/yarn.list
