#!/bin/bash

# Auto-commit changes for Spec Kit
# Usage: ./auto-commit.sh <event_name>

set -e

EVENT_NAME="$1"
CONFIG_FILE=".specify/extensions/git/git-config.yml"

# Check if git is available
if ! command -v git &> /dev/null; then
    echo "Warning: Git is not available. Skipping auto-commit."
    exit 0
fi

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "Warning: Not in a git repository. Skipping auto-commit."
    exit 0
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Warning: Config file not found at $CONFIG_FILE. Skipping auto-commit."
    exit 0
fi

# Parse YAML config (simple approach)
get_config_value() {
    local key="$1"
    local default="$2"
    local value
    
    # Try to get event-specific config first
    value=$(grep -A2 "^  $EVENT_NAME:" "$CONFIG_FILE" | grep "enabled:" | awk '{print $2}' | tr -d '[:space:]' 2>/dev/null || true)
    
    if [ -z "$value" ] || [ "$value" = "null" ]; then
        # Fall back to default
        value=$(grep "^  default:" "$CONFIG_FILE" | awk '{print $2}' | tr -d '[:space:]' 2>/dev/null || true)
    fi
    
    if [ -z "$value" ] || [ "$value" = "null" ]; then
        echo "$default"
    else
        echo "$value"
    fi
}

get_commit_message() {
    local event="$1"
    
    # Try to get event-specific message
    local message=$(grep -A2 "^  $event:" "$CONFIG_FILE" | grep "message:" | cut -d: -f2- | sed 's/^[[:space:]]*//' 2>/dev/null || true)
    
    if [ -z "$message" ] || [ "$message" = "null" ]; then
        # Default message
        echo "[Spec Kit] $event"
    else
        echo "$message"
    fi
}

# Check if auto-commit is enabled for this event
ENABLED=$(get_config_value "$EVENT_NAME" "false")

if [ "$ENABLED" != "true" ]; then
    echo "Auto-commit is disabled for event: $EVENT_NAME"
    exit 0
fi

# Check if there are any changes to commit
if git diff --quiet && git diff --cached --quiet; then
    echo "No changes to commit for event: $EVENT_NAME"
    exit 0
fi

# Get commit message
COMMIT_MESSAGE=$(get_commit_message "$EVENT_NAME")

echo "Auto-committing changes for event: $EVENT_NAME"
echo "Commit message: $COMMIT_MESSAGE"

# Stage all changes
git add .

# Create commit
git commit -m "$COMMIT_MESSAGE"

echo "Successfully committed changes for event: $EVENT_NAME"