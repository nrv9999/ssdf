#!/usr/bin/env bash
# Install the ssdf Python scripts into /opt/ssdf as executable .py files
# (shebang #!/usr/bin/env python3 + exec bit), so they run directly:
#   /opt/ssdf/ssdf.py --mode strict     (from the install folder: ./ssdf.py)
# The database is NOT touched: apply framework/install.sql and example/install.sql
# via psql yourself (see README).
# Requires psycopg (v3) for Python 3. Run with sudo:
#   sudo bash install.sh [--path]
#
# --path : also symlink the scripts into /usr/local/bin (already on PATH in any
#          shell, fish included), keeping the .py names: ssdf.py,
#          ssdf_admin_clean_err_tail.py -> callable from any folder.
#          Without --path the symlinks are removed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
WITH_PATH=0
for arg in "$@"; do
	case "$arg" in
		--path) WITH_PATH=1 ;;
		*) echo "unknown option: $arg" >&2; exit 1 ;;
	esac
done

install -d /opt/ssdf/test/example
install -m 755 "$ROOT/framework/python/ssdf.py" /opt/ssdf/ssdf.py
install -m 755 "$ROOT/framework/python/ssdf_admin_clean_err_tail.py" /opt/ssdf/ssdf_admin_clean_err_tail.py
install -m 755 "$ROOT/example/test/python/all_execs.py" /opt/ssdf/test/example/all_execs.py

CMDS="ssdf.py ssdf_admin_clean_err_tail.py"
# artifacts of earlier installer versions
rm -f /usr/local/bin/ssdf /usr/local/bin/ssdf_admin_clean_err_tail /usr/local/bin/ssdf-admin /usr/local/bin/ssdf-test
rm -f /etc/profile.d/ssdf.sh

for cmd in $CMDS; do
	if [ "$WITH_PATH" = 1 ]; then
		ln -sf "/opt/ssdf/$cmd" "/usr/local/bin/$cmd"
	else
		rm -f "/usr/local/bin/$cmd"
	fi
done

echo "installed: /opt/ssdf (executable .py)"
if [ "$WITH_PATH" = 1 ]; then
	echo "from any folder: $CMDS (symlinks in /usr/local/bin)"
else
	echo "run: /opt/ssdf/ssdf.py ... (--path to call ssdf.py from any folder)"
fi
echo "test: /opt/ssdf/test/example/all_execs.py [--no-reset]"
echo "database: apply framework/install.sql and example/install.sql via psql (see README)"
