#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
service_label="com.local.system-metric-collector"
domain="gui/$(id -u)"
service_plist="$HOME/Library/LaunchAgents/$service_label.plist"
service_wrapper="$project_dir/.collector-launchd-wrapper.sh"

launchctl bootout "$domain/$service_label" 2>/dev/null || true
if [ -f "$service_plist" ]; then
  rm "$service_plist"
fi
if [ -f "$service_wrapper" ]; then
  rm "$service_wrapper"
fi
echo "Uninstalled $service_label (collector logs were preserved)."

