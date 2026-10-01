#!/bin/sh
set -eu

project_dir="__PROJECT_DIR__"
python_executable="__PYTHON_EXECUTABLE__"
if [ ! -f "$project_dir/.env" ]; then
  echo "Missing collector environment file: $project_dir/.env" >&2
  exit 1
fi

cd "$project_dir"
exec "$python_executable" -m collector
