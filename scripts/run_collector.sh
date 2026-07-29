#!/bin/sh
set -eu

project_dir="__PROJECT_DIR__"
python_executable="__PYTHON_EXECUTABLE__"
environment_file="$project_dir/.env"

if [ ! -f "$environment_file" ]; then
  echo "Missing collector environment file: $environment_file" >&2
  exit 1
fi

set -a
# The file is maintained by the local user and contains shell-compatible KEY=VALUE entries.
. "$environment_file"
set +a

cd "$project_dir"
exec "$python_executable" -m collector

