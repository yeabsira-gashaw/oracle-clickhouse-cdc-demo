#!/bin/sh
set -eu
BASE="${CONNECT_URL:-http://localhost:8083}"
until curl -fsS "$BASE/connectors" >/dev/null; do sleep 3; done
curl --fail-with-body -sS -X PUT "$BASE/connectors/oracle-customers/config" \
  -H 'Content-Type: application/json' \
  --data-binary @"$(dirname "$0")/../connector/oracle-customers.json"
printf '\n'
