#!/usr/bin/env bash
set -euo pipefail

exec /opt/render/project/src/.venv/bin/gunicorn app:app
