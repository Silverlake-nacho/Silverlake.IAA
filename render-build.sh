#!/usr/bin/env bash
set -euo pipefail

apt-get update
apt-get install -y curl apt-transport-https gnupg2

curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/11/prod.list \
  > /etc/apt/sources.list.d/mssql-release.list

apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

python3 -m pip install --upgrade pip
python3 -m pip install -r /opt/render/project/src/requirements.txt
