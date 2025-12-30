#!/bin/bash
# Trino entrypoint - 환경변수를 config.properties에 주입

CONFIG_DIR="/etc/trino"
TEMPLATE="${CONFIG_DIR}/config.properties.template"
CONFIG="${CONFIG_DIR}/config.properties"

if [ -f "$TEMPLATE" ]; then
    cp "$TEMPLATE" "$CONFIG"
    sed -i "s|\${TRINO_QUERY_MAX_MEMORY}|${TRINO_QUERY_MAX_MEMORY:-4GB}|g" "$CONFIG"
    sed -i "s|\${TRINO_QUERY_MAX_MEMORY_PER_NODE_COORDINATOR}|${TRINO_QUERY_MAX_MEMORY_PER_NODE_COORDINATOR:-1GB}|g" "$CONFIG"
    sed -i "s|\${TRINO_QUERY_MAX_MEMORY_PER_NODE_WORKER}|${TRINO_QUERY_MAX_MEMORY_PER_NODE_WORKER:-512MB}|g" "$CONFIG"
    echo "Generated config.properties:"
    cat "$CONFIG"
fi

# Trino 시작
exec /usr/lib/trino/bin/run-trino
