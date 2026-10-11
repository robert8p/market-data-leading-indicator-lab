#!/usr/bin/env bash
# Approved isolated verification; no operational entrypoint or deployment.
set -euo pipefail
test "${GITHUB_REPOSITORY:-}" = robert8p/market-data-leading-indicator-lab
test "${GITHUB_REF:-}" = refs/heads/ci/astra-offline-candidate-20261011
test "$(git rev-parse HEAD^)" = fa0d261d9a76687e2308221955c67d5cb8246271
stage="$(mktemp -d "${RUNNER_TEMP:?}/astra-ci.XXXXXX")"
cidfile="$stage/container.id"
cleanup() {
  if test -s "$cidfile"; then docker rm -f "$(cat "$cidfile")" >/dev/null 2>&1 || true; fi
  rm -rf -- "$stage"
}
trap cleanup EXIT
mkdir "$stage/source" "$stage/deps" "$stage/harness"
git archive HEAD | tar -x -C "$stage/source"
cp requirements.txt "$stage/deps/requirements.txt"
cp .astra-ci/Dependency.Dockerfile "$stage/deps/Dockerfile"
cp .astra-ci/inside-ci.py .astra-ci/full-candidate-hashes.json "$stage/harness/"
docker pull python:3.12-slim
image_ref="$(docker image inspect python:3.12-slim --format '{{index .RepoDigests 0}}')"
printf 'BASE_IMAGE=%s\n' "$image_ref"
docker build --pull=false --build-arg "BASE_IMAGE=$image_ref" -t "astra-ci:$GITHUB_RUN_ID" "$stage/deps"
docker image inspect "astra-ci:$GITHUB_RUN_ID" --format 'DEPENDENCY_IMAGE={{.Id}}'
set +e
timeout --signal=TERM --kill-after=10s 900s docker run --init --cidfile "$cidfile" \
  --network none --read-only --user 10001:10001 --cap-drop ALL \
  --security-opt no-new-privileges --pids-limit 128 --memory 2g --cpus 2 \
  --ulimit nofile=1024:1024 --ulimit fsize=67108864:67108864 \
  --tmpfs /work:rw,nosuid,nodev,size=1073741824,uid=10001,gid=10001 \
  --tmpfs /tmp:rw,nosuid,nodev,size=268435456,uid=10001,gid=10001 \
  --mount "type=bind,src=$stage/source,dst=/input,readonly" \
  --mount "type=bind,src=$stage/harness,dst=/harness,readonly" \
  --workdir /work "astra-ci:$GITHUB_RUN_ID" \
  env -i PATH=/usr/local/bin:/usr/bin:/bin LANG=C.UTF-8 \
  python -I -S /harness/inside-ci.py
status=$?
set -e
if test -s "$cidfile"; then
  docker inspect "$(cat "$cidfile")" --format 'BOUNDARY={{json .HostConfig}} STATE={{json .State}}'
fi
exit "$status"
