#!/usr/bin/env bash
# One-shot setup for Debian 12/13 (run from the project folder). Only touches apt if something is missing.
set -euo pipefail
need=()
ls /usr/lib/jvm 2>/dev/null | grep -Eq 'java-(1[7-9]|[2-9][0-9])' || need+=(default-jre-headless)
python3 -c "import ensurepip, venv" 2>/dev/null || need+=(python3-venv)
if [ ${#need[@]} -gt 0 ]; then
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends "${need[@]}"
fi
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
[ -f logs/access.log ] || .venv/bin/python generate_logs.py
echo
echo "Done. Start with:  .venv/bin/python dashboard.py"
echo "Then open  http://$(hostname -I | awk '{print $1}'):8050  from Windows."
