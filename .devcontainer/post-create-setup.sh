#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

echo "Starting post-create setup..."

# dbt-core, dbt-postgres and dbt-duckdb are installed here early so it's picked up by the dbt vscode extension.
pip install --upgrade pip
pip install dbt-core dbt-postgres dbt-duckdb duckcli
pip3 install --user -r requirements.txt
yes | npx npx wrangler login --browser=false --callback-host=0.0.0.0 --callback-port=8976 | stdbuf -oL sed 's/0\.0\.0\.0/localhost/g'
echo "INFO: Wrangler login process initiated."

# Doppler login and setup
if doppler whoami &> /dev/null; then
    echo "Already logged in to Doppler."
else
    echo "Logging into Doppler..."
    doppler login --no-check-version --no-timeout --yes 
    doppler setup --no-interactiveå
fi

if [ -f ".devcontainer/setup-postgres.sh" ]; then
    sudo bash .devcontainer/setup-postgres.sh
else
    echo "INFO: .devcontainer/setup-postgres.sh not found, skipping PostgreSQL setup."
fi

echo "INFO: Starting Zsh and Oh My Zsh setup..."

# Determine the target non-root user and their home directory for Zsh setup.
TARGET_USER=""
TARGET_USER_HOME=""

# 1. Try environment variables like _REMOTE_USER and _REMOTE_USER_HOME
if [ -n "$_REMOTE_USER" ]; then
    TARGET_USER="$_REMOTE_USER"
    if [ -n "$_REMOTE_USER_HOME" ]; then
        TARGET_USER_HOME="$_REMOTE_USER_HOME"
    else
        TARGET_USER_HOME=$(getent passwd "$TARGET_USER" | cut -d: -f6)
    fi
fi

# 2. If not found, try the user with UID 1000
if [ -z "$TARGET_USER" ] || [ -z "$TARGET_USER_HOME" ]; then
    CANDIDATE_USER_UID="1000"
    CANDIDATE_USER=$(getent passwd "$CANDIDATE_USER_UID" 2>/dev/null | cut -d: -f1)
    if [ -n "$CANDIDATE_USER" ]; then
        TARGET_USER="$CANDIDATE_USER"
        TARGET_USER_HOME=$(getent passwd "$TARGET_USER" | cut -d: -f6)
    fi
fi

# 3. If still not found, and 'vscode' user exists (common in VS Code dev containers)
if [ -z "$TARGET_USER" ] || [ -z "$TARGET_USER_HOME" ]; then
    if id "vscode" &>/dev/null; then
        TARGET_USER="vscode"
        TARGET_USER_HOME=$(getent passwd "vscode" | cut -d: -f6)
    fi
fi

# 4. If script is run by a non-root user and target still undetermined, use current user
if [ -z "$TARGET_USER" ] || [ -z "$TARGET_USER_HOME" ]; then
    if [ "$(id -u)" -ne 0 ] && [ -n "$USER" ] && [ -n "$HOME" ]; then
        TARGET_USER="$USER"
        TARGET_USER_HOME="$HOME"
    fi
fi

if [ -n "$TARGET_USER" ] && [ -n "$TARGET_USER_HOME" ] && [ -d "$TARGET_USER_HOME" ]; then
    echo "INFO: Configuring Zsh for user '$TARGET_USER' in home directory '$TARGET_USER_HOME'."

    echo "INFO: Creating Oh My Zsh custom directories for $TARGET_USER..."
    mkdir -p "$TARGET_USER_HOME/.oh-my-zsh/custom/themes" "$TARGET_USER_HOME/.oh-my-zsh/custom/plugins"
    if [ -d "$TARGET_USER_HOME/.oh-my-zsh" ]; then
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$TARGET_USER_HOME/.oh-my-zsh"
    fi

    if [ -f "/workspaces/ftn/.devcontainer/.zshrc" ]; then
        echo "INFO: Copying .zshrc to $TARGET_USER_HOME/.zshrc"
        cp "/workspaces/ftn/.devcontainer/.zshrc" "$TARGET_USER_HOME/.zshrc"
        sudo chown "$TARGET_USER:$TARGET_USER" "$TARGET_USER_HOME/.zshrc"
    else
        echo "INFO: /workspaces/ftn/.devcontainer/.zshrc not found, skipping copy."
    fi

    if [ -f "/workspaces/ftn/.devcontainer/.p10k.zsh" ]; then
        echo "INFO: Copying .p10k.zsh to $TARGET_USER_HOME/.p10k.zsh"
        cp "/workspaces/ftn/.devcontainer/.p10k.zsh" "$TARGET_USER_HOME/.p10k.zsh"
        sudo chown "$TARGET_USER:$TARGET_USER" "$TARGET_USER_HOME/.p10k.zsh"
    else
        echo "INFO: /workspaces/ftn/.devcontainer/.p10k.zsh not found, skipping copy."
    fi

    ZSH_CUSTOM_DIR="$TARGET_USER_HOME/.oh-my-zsh/custom"
    POWERLEVEL10K_DIR="$ZSH_CUSTOM_DIR/themes/powerlevel10k"
    ZSH_SYNTAX_HIGHLIGHTING_DIR="$ZSH_CUSTOM_DIR/plugins/zsh-syntax-highlighting"
    ZSH_AUTOSUGGESTIONS_DIR="$ZSH_CUSTOM_DIR/plugins/zsh-autosuggestions"

    if [ ! -d "$POWERLEVEL10K_DIR" ]; then
        echo "INFO: Cloning Powerlevel10k theme to $POWERLEVEL10K_DIR..."
        git clone --depth=1 https://github.com/romkatv/powerlevel10k.git "$POWERLEVEL10K_DIR"
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$POWERLEVEL10K_DIR"
    else
        echo "INFO: Powerlevel10k theme already exists at $POWERLEVEL10K_DIR. Ensuring correct ownership."
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$POWERLEVEL10K_DIR"
    fi

    if [ ! -d "$ZSH_SYNTAX_HIGHLIGHTING_DIR" ]; then
        echo "INFO: Cloning zsh-syntax-highlighting plugin to $ZSH_SYNTAX_HIGHLIGHTING_DIR..."
        git clone https://github.com/zsh-users/zsh-syntax-highlighting.git "$ZSH_SYNTAX_HIGHLIGHTING_DIR"
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$ZSH_SYNTAX_HIGHLIGHTING_DIR"
    else
        echo "INFO: zsh-syntax-highlighting plugin already exists at $ZSH_SYNTAX_HIGHLIGHTING_DIR. Ensuring correct ownership."
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$ZSH_SYNTAX_HIGHLIGHTING_DIR"
    fi

    if [ ! -d "$ZSH_AUTOSUGGESTIONS_DIR" ]; then
        echo "INFO: Cloning zsh-autosuggestions plugin to $ZSH_AUTOSUGGESTIONS_DIR..."
        git clone https://github.com/zsh-users/zsh-autosuggestions.git "$ZSH_AUTOSUGGESTIONS_DIR"
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$ZSH_AUTOSUGGESTIONS_DIR"
    else
        echo "INFO: zsh-autosuggestions plugin already exists at $ZSH_AUTOSUGGESTIONS_DIR. Ensuring correct ownership."
        sudo chown -R "$TARGET_USER:$TARGET_USER" "$ZSH_AUTOSUGGESTIONS_DIR"
    fi
else
    echo "WARNING: Could not reliably determine target user or home directory for Zsh setup. Skipping Zsh specific configurations."
fi

echo "Post-create setup finished successfully!"