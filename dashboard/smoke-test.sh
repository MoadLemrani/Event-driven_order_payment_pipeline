#!/bin/sh
# Exercise the nginx image, including SPA fallback, without running backend services.
set -eu
image="order-pipeline-dashboard-smoke"
container="order-pipeline-dashboard-smoke-$$"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM
docker build -t "$image" .
docker run -d --name "$container" -p 127.0.0.1::80 \
  --add-host order-service:127.0.0.1 --add-host inventory-service:127.0.0.1 \
  --add-host payment-service:127.0.0.1 "$image" >/dev/null
address=$(docker port "$container" 80/tcp | head -n 1)
base="http://$address"
ready=false
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if index=$(curl -fsS "$base/" 2>/dev/null); then ready=true; break; fi
  sleep 1
done
if [ "$ready" != true ]; then docker logs "$container"; exit 1; fi
printf '%s' "$index" | grep -q '<div id="root"></div>'
asset=$(printf '%s' "$index" | sed -n 's/.*src="\([^"]*\.js\)".*/\1/p')
[ -n "$asset" ]
curl -fsS "$base$asset" >/dev/null
curl -fsS "$base/orders/example" | grep -q '<div id="root"></div>'
printf 'Dashboard HTML, JavaScript asset, and SPA fallback are served.\n'
