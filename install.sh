#!/usr/bin/env bash
# Install the ssdf Python dispatcher into /opt/ssdf and put wrappers on PATH.
# Requires python-psycopg (sudo pacman -S python-psycopg). Run with sudo:
#   sudo bash install.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

install -d /opt/ssdf/test
install -m 644 "$ROOT/framework/python/ssdf.py" /opt/ssdf/ssdf.py
install -m 644 "$ROOT/example/test/python/all_execs.py" /opt/ssdf/test/all_execs.py

printf '#!/bin/sh\nexec python3 /opt/ssdf/ssdf.py "$@"\n' > /usr/local/bin/ssdf
printf '#!/bin/sh\nexec python3 /opt/ssdf/test/all_execs.py "$@"\n' > /usr/local/bin/ssdf-test
chmod 755 /usr/local/bin/ssdf /usr/local/bin/ssdf-test

echo "installed: /opt/ssdf/ssdf.py, /opt/ssdf/test/all_execs.py"
echo "on PATH: ssdf (dispatcher), ssdf-test (test scenarios)"
