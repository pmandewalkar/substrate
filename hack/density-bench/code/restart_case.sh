#!/usr/bin/env bash
# Restart one case of the benchmark driver from a given test on the existing deployment.
# usage: restart_case.sh <case> <from: T1|T2> [--redeploy]
set -u
CASE="$1"; FROM="$2"; REDEPLOY="${3:-}"
RID=$(cat ~/bench-runs/driver/current_run_id)
OUT=~/bench-runs/results/$RID/$CASE
export KUBECONFIG=$OUT/kubeconfig
for p in $(pgrep -f "python3 bench_driver.py $CASE"); do kill "$p"; done
sleep 2
for j in $(kubectl get jobs -n benchmarking -o name | grep -i "sw-${CASE//_/-}-"); do
  st=$(kubectl get -n benchmarking "$j" -o jsonpath='{.status.active}')
  [ -n "$st" ] && kubectl delete -n benchmarking "$j" --wait=true --timeout=180s
done
sleep 20
cd ~/bench-runs/driver
ARGS="--run-id $RID --from $FROM"
[ "$REDEPLOY" != "--redeploy" ] && ARGS="$ARGS --skip-deploy"
setsid nohup python3 bench_driver.py "$CASE" $ARGS >> ~/bench-runs/results/$RID/$CASE.console.log 2>&1 < /dev/null &
echo "restarted $CASE from $FROM (run $RID)"
