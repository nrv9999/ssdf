#!/usr/bin/env bash
# Remove the ssdf Python scripts installed by install.sh. Run with sudo:
#   sudo bash uninstall.sh
# The python-psycopg package is left in place; remove it separately if needed:
#   sudo pacman -Rs python-psycopg
set -euo pipefail

rm -f /usr/local/bin/ssdf.py /usr/local/bin/ssdf_admin_clean_err_tail.py
rm -f /usr/local/bin/ssdf /usr/local/bin/ssdf_admin_clean_err_tail /usr/local/bin/ssdf-admin /usr/local/bin/ssdf-test   # legacy
rm -f /etc/profile.d/ssdf.sh   # legacy
rm -rf /opt/ssdf

echo "removed: /opt/ssdf, /usr/local/bin symlinks (ssdf.py, ssdf_admin_clean_err_tail.py)"
