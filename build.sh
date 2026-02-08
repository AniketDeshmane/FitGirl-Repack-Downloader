#!/usr/bin/env bash
# Install dependencies and run Download Manager (Linux/macOS/Git Bash)
set -e
cd "$(dirname "$0")"

PYTHON=""
for p in python3 python; do
  if command -v "$p" &>/dev/null; then
    PYTHON="$p"
    break
  fi
done
if [ -z "$PYTHON" ]; then
  echo "Python 3 not found. Install Python 3 and try again."
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "Creating venv..."
  "$PYTHON" -m venv venv
fi
source venv/bin/activate

echo "Installing dependencies..."
pip install -q -r requirements.txt

echo "Starting Download Manager..."
exec "$PYTHON" download_manager.py
