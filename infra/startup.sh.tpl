#!/bin/bash
set -euo pipefail
exec > /var/log/mpk-startup.log 2>&1

echo "=== MPK Airflow startup: $(date) ==="

# ── Install Docker (arm64) ─────────────────────────────────────────────────────
apt-get update -y
apt-get install -y ca-certificates curl gnupg git python3 gettext-base

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=arm64 signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu jammy stable" \
  > /etc/apt/sources.list.d/docker.list

apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu

echo "Docker installed: $(docker --version)"

# ── Clone repo ─────────────────────────────────────────────────────────────────
APP_DIR=/opt/mpk
git clone ${repo_url} $APP_DIR
chown -R ubuntu:ubuntu $APP_DIR
cd $APP_DIR

# ── Create required directories (mirrors deploy.yml) ──────────────────────────
mkdir -p ./dags ./logs ./plugins ./config
chown -R ubuntu:ubuntu ./dags ./logs ./plugins ./config

# ── Generate dbt profiles.yml (mirrors deploy.yml python fallback) ────────────
export DBT_USER="${dbt_user}"
export DBT_PASSWORD="${dbt_password}"
export DBT_ACCOUNT="${dbt_account}"
export DBT_DATABASE="${dbt_database}"
export DBT_SCHEMA="${dbt_schema}"
export DBT_WAREHOUSE="${dbt_warehouse}"
export DBT_WAREHOUSE_STRONK="${dbt_warehouse_stronk}"
export DBT_ROLE="${dbt_role}"

python3 - <<'PYEOF'
import os
from string import Template
t = Template(open('dbt/test-profiles.yml').read())
open('dbt/profiles.yml', 'w').write(t.safe_substitute(os.environ))
print("dbt profiles.yml generated")
PYEOF

# ── Write .env (mirrors deploy.yml) ───────────────────────────────────────────
AIRFLOW_UID=$(id -u ubuntu)

cat > .env <<EOF
AIRFLOW_UID=$AIRFLOW_UID
POSTGRES_USER=${postgres_user}
POSTGRES_PASSWORD=${postgres_password}
POSTGRES_DB=${postgres_db}
AIRFLOW_FERNET_KEY=${airflow_fernet_key}
DBT_USER=${dbt_user}
DBT_PASSWORD=${dbt_password}
DBT_ACCOUNT=${dbt_account}
DBT_DATABASE=${dbt_database}
DBT_SCHEMA=${dbt_schema}
DBT_WAREHOUSE=${dbt_warehouse}
DBT_WAREHOUSE_STRONK=${dbt_warehouse_stronk}
DBT_ROLE=${dbt_role}
TOM_TOM_API_KEY=${tomtom_api_key}
EOF

echo ".env written"

# ── Init Airflow DB (mirrors deploy.yml) ──────────────────────────────────────
docker compose up airflow-init --exit-code-from airflow-init
echo "Airflow DB init complete"

# ── Deploy (mirrors deploy.yml) ───────────────────────────────────────────────
docker compose up -d --build --remove-orphans
echo "=== Startup complete: $(date) ==="
