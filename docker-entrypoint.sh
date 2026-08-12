#!/bin/sh
set -eu

data_dir="${RIGPULSE_DATA_DIR:-/data}"
mkdir -p "$data_dir"
chown -R rigpulse:rigpulse "$data_dir"

exec gosu rigpulse "$@"
