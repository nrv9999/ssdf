#!/usr/bin/env bash
# Remove the ssdf Python dispatcher installed by install.sh. Run with sudo:
#   sudo bash uninstall.sh
# The python-psycopg package is left in place; remove it separately if needed:
#   sudo pacman -Rs python-psycopg
set -euo pipefail

rm -f /usr/local/bin/ssdf /usr/local/bin/ssdf-test
rm -rf /opt/ssdf

echo "removed: /opt/ssdf, /usr/local/bin/ssdf, /usr/local/bin/ssdf-test"
