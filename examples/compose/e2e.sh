#!/usr/bin/env sh
# End-to-end check of the compose stack: create an agent, run it, restart
# fiber, and prove the definition, its revisions and the run ledger survived in Postgres.
#
# Run from the repository root: sh examples/compose/e2e.sh
set -eu

m() { docker compose exec -T fiber fiber "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

docker compose up --build --wait

# A fresh name per run, so the script is repeatable against a kept volume.
name="e2e-$(date +%s)"
spec=$(sed "s/\"docs-bot\"/\"$name\"/" examples/compose/agent.json)

echo "== create $name"
echo "$spec" | m agent create -f /dev/stdin
echo "== revise $name"
echo "$spec" | sed 's/about the project/about the project, briefly/' | m agent revise -f /dev/stdin

echo "== tag $name"
m tag "$name" "are you there?" | tee /dev/stderr | grep -q "offline provider" || fail "the run produced no answer"

runs() { docker compose exec -T postgres psql -U fiber -d fiber -tAc "SELECT count(*) FROM mandatum_runs"; }
before=$(runs)
[ "$before" -ge 1 ] || fail "the run was not recorded in the Postgres ledger"

echo "== restart fiber"
docker compose restart fiber
docker compose up --wait fiber

echo "== read $name back"
m agent history "$name" --format json | grep -c '"rev"' | grep -qx 2 || fail "expected 2 revisions after restart"
m tag "$name" "still there?" | grep -q "offline provider" || fail "the agent did not run after restart"

[ "$(runs)" -gt "$before" ] || fail "the ledger did not keep the earlier run and add the new one"

echo "== index docs and ask a grounded agent"
docker compose cp docs/architecture fiber:/tmp/handbook >/dev/null
m index --source handbook /tmp/handbook
grounded=$(sed "s/\"docs-bot\"/\"$name-grounded\"/" examples/compose/grounded.json)
echo "$grounded" | m agent create -f /dev/stdin
m tag "$name-grounded" "how does a tag reach a sink through the router?" --format json \
	| grep -q "handbook" || fail "the grounded run cited nothing from the handbook source"

echo "== send a file with a request"
docker compose cp examples/compose/agent.json fiber:/tmp/attached.json >/dev/null
m tag "$name" "what is in this file?" --attach /tmp/attached.json | grep -q "offline provider" \
	|| fail "a run with an attached file did not complete"

entries=$(docker compose exec -T postgres psql -U fiber -d fiber -tAc "SELECT count(*) FROM mandatum_thread_entries")
[ "$entries" -ge 2 ] || fail "the conversation journal recorded nothing"

echo "PASS: $name kept both revisions across a restart and still runs"
