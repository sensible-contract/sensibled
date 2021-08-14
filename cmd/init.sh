#!/usr/bin/env sh

# init all data by small step

set -o errexit

_term() {
  echo "Caught SIGTERM signal!"
  kill -TERM "$child" 2>/dev/null
  wait "$child"
}

trap _term SIGTERM
trap _term SIGINT

set -v on

./sensibled -full -end 100000 &
child=$!
wait "$child"

BLOCK_HEIGHT_STEPS="200000 300000 350000 400000 450000 500000 550000 600000 650000 690679"
for BLOCK_HEIGHT in $BLOCK_HEIGHT_STEPS; do
    echo $BLOCK_HEIGHT
    ./sensibled -end $BLOCK_HEIGHT &
    child=$!
    wait "$child"
done
