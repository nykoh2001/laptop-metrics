#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
python_executable="$project_dir/.venv/bin/python"
template="$project_dir/launchd/com.local.security-telemetry-collector.plist.example"
wrapper_template="$project_dir/scripts/run_collector.sh"
launch_agents_dir="$HOME/Library/LaunchAgents"
logs_dir="$HOME/Library/Logs/laptop-metrics"
service_plist="$launch_agents_dir/com.local.security-telemetry-collector.plist"
service_wrapper="$project_dir/.collector-launchd-wrapper.sh"
service_label="com.local.security-telemetry-collector"
legacy_service_label="com.local.system-metric-collector"
legacy_service_plist="$launch_agents_dir/$legacy_service_label.plist"
domain="gui/$(id -u)"

if [ ! -x "$python_executable" ]; then
  echo "Missing virtual-environment Python: $python_executable" >&2
  echo "Create it with: python3.11 -m venv \"$project_dir/.venv\"" >&2
  exit 1
fi

if [ ! -f "$project_dir/collector/__main__.py" ]; then
  echo "collector/__main__.py is not implemented yet; refusing to install a restart loop." >&2
  exit 1
fi

if [ ! -f "$project_dir/.env" ]; then
  echo "Missing $project_dir/.env; copy and edit .env.example first." >&2
  exit 1
fi

mkdir -p "$launch_agents_dir" "$logs_dir"
sed -e "s|__PROJECT_DIR__|$project_dir|g" \
  -e "s|__PYTHON_EXECUTABLE__|$python_executable|g" \
  "$wrapper_template" > "$service_wrapper"
chmod 700 "$service_wrapper"

sed -e "s|__PROJECT_DIR__|$project_dir|g" \
  -e "s|__HOME_DIR__|$HOME|g" \
  "$template" > "$service_plist"
plutil -lint "$service_plist"

launchctl bootout "$domain/$legacy_service_label" 2>/dev/null || true
if [ -f "$legacy_service_plist" ]; then
  rm "$legacy_service_plist"
  echo "Removed legacy $legacy_service_label launch agent."
fi
launchctl bootout "$domain/$service_label" 2>/dev/null || true
launchctl bootstrap "$domain" "$service_plist"
launchctl enable "$domain/$service_label"
launchctl kickstart "$domain/$service_label"
echo "Installed $service_label"
echo "Logs: $logs_dir"
