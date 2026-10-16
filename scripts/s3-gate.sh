#!/usr/bin/env bash
# Conditional-PUT gate against the local RustFS. Writes results/rustfs-gate.txt.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
OUT="$ROOT/results/rustfs-gate.txt"
IMAGE_DIGEST="sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff"

if [ ! -x "$HERE/rustfs.sh" ] || ! eval "$("$HERE/rustfs.sh" env 2>/dev/null)" 2>/dev/null \
   || ! curl -sS -o /dev/null --max-time 5 "$DURABLE_ENDPOINT/health" 2>/dev/null; then
  echo "RustFS is not running (scripts/rustfs.sh up)" >&2
  exit 2
fi

AK="$DURABLE_ACCESS_KEY"; SK="$DURABLE_SECRET_KEY"
BASE="$DURABLE_ENDPOINT/$DURABLE_BUCKET"
PREFIX="gate/$(date +%s)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/s3gate.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# s3 METHOD URL OUTFILE [curl args...] -> prints HTTP code; headers in OUTFILE.h, body in OUTFILE
s3() {
  local method="$1" url="$2" out="$3"; shift 3
  curl -sS -X "$method" --aws-sigv4 "aws:amz:${DURABLE_REGION}:s3" --user "$AK:$SK" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -D "$out.h" -o "$out" -w '%{http_code}' "$@" "$url"
}
etag_of() { awk -F': ' 'tolower($1)=="etag"{gsub(/\r/,"",$2);print $2}' "$1.h"; }

: > "$TMP/results"
FAILED=0
rec() { # status name detail [critical]
  echo "$1 $2: $3" >> "$TMP/results"
  if [ "$1" = FAIL ] && [ "${4:-1}" = 1 ]; then FAILED=1; fi
  return 0
}

# 1 create_if_absent
K="$BASE/$PREFIX/c1"
c1=$(s3 PUT "$K" "$TMP/a" -H "If-None-Match: *" --data-binary "first")
E1="$(etag_of "$TMP/a")"
c2=$(s3 PUT "$K" "$TMP/b" -H "If-None-Match: *" --data-binary "second")
c3=$(s3 GET "$K" "$TMP/g"); body="$(cat "$TMP/g")"
if [ "$c1" = 200 ] && [ "$c2" = 412 ] && [ "$body" = first ]; then
  rec PASS create_if_absent "first=$c1 second=$c2 get_body=$body"
else
  rec FAIL create_if_absent "first=$c1 second=$c2 get=$c3 get_body=$body"
fi

# 2 create_race
K="$BASE/$PREFIX/race"
for i in $(seq 1 20); do
  ( s3 PUT "$K" "$TMP/r$i" -H "If-None-Match: *" --data-binary "body-$i" > "$TMP/rc$i" ) &
done
wait
w200=0; w412=0; other=""; winner=""
for i in $(seq 1 20); do
  c="$(cat "$TMP/rc$i")"
  case "$c" in 200) w200=$((w200+1)); winner="body-$i";; 412) w412=$((w412+1));; *) other="$other $c";; esac
done
s3 GET "$K" "$TMP/rg" >/dev/null; rbody="$(cat "$TMP/rg")"
if [ "$w200" = 1 ] && [ "$w412" = 19 ] && [ "$rbody" = "$winner" ]; then
  rec PASS create_race "200x$w200 412x$w412 get_body=$rbody winner=$winner"
else
  rec FAIL create_race "200x$w200 412x$w412 other=[$other] get_body=$rbody winner=$winner"
fi

# 3 if_match (uses key c1 and E1 from check 1)
K="$BASE/$PREFIX/c1"
m1=$(s3 PUT "$K" "$TMP/m1" -H "If-Match: $E1" --data-binary "updated")
E2="$(etag_of "$TMP/m1")"
m2=$(s3 PUT "$K" "$TMP/m2" -H "If-Match: $E1" --data-binary "stale")
m3=$(s3 PUT "$BASE/$PREFIX/missing" "$TMP/m3" -H "If-Match: $E1" --data-binary "x")
s3 GET "$K" "$TMP/mg" >/dev/null
if [ "$m1" = 200 ] && [ -n "$E2" ] && [ "$E2" != "$E1" ] && [ "$m2" = 412 ] && { [ "$m3" = 412 ] || [ "$m3" = 404 ]; } \
   && [ "$(cat "$TMP/mg")" = updated ]; then
  rec PASS if_match "match=$m1 etag1=$E1 etag2=$E2 stale=$m2 missing_key=$m3"
else
  rec FAIL if_match "match=$m1 etag1=$E1 etag2=$E2 stale=$m2 missing_key=$m3"
fi

# 4 etag_header: all 200 PUTs above carried ETag
missing=""
for f in a m1 $(seq -f 'r%g' 1 20); do
  [ -f "$TMP/$f.h" ] || continue
  if head -1 "$TMP/$f.h" | grep -q ' 200'; then
    [ -n "$(etag_of "$TMP/$f")" ] || missing="$missing $f"
  fi
done
[ -n "$(etag_of "$TMP/a")" ] && [ -z "$missing" ] \
  && rec PASS etag_header "every 200 PUT carried ETag (e.g. $E1)" \
  || rec FAIL etag_header "200 PUTs without ETag:[$missing]"

# 5 list_delete (informational)
l=$(s3 GET "$BASE?list-type=2&prefix=$PREFIX/" "$TMP/l")
nkeys=$(grep -o '<Key>' "$TMP/l" | wc -l | tr -d ' ')
d=$(s3 DELETE "$BASE/$PREFIX/c1" "$TMP/d")
[ "$l" = 200 ] && [ "$nkeys" -ge 2 ] && [ "$d" = 204 ] \
  && rec PASS list_delete "list=$l keys=$nkeys delete=$d" 0 \
  || rec FAIL list_delete "list=$l keys=$nkeys delete=$d" 0

# 6 pause (informational)
if podman pause turso-durable-rustfs >/dev/null 2>&1; then
  pu=ok
else pu=failed; fi
podman unpause turso-durable-rustfs >/dev/null 2>&1 && un=ok || un=failed
sleep 1
pg=$(s3 GET "$BASE/$PREFIX/race" "$TMP/pg")
[ "$pu" = ok ] && [ "$un" = ok ] && [ "$pg" = 200 ] \
  && rec PASS pause "pause=$pu unpause=$un get_after=$pg" 0 \
  || rec FAIL pause "pause=$pu unpause=$un get_after=$pg" 0

{
  echo "# RustFS conditional-PUT gate"
  echo "# image: docker.io/rustfs/rustfs:1.0.0@$IMAGE_DIGEST"
  echo "# curl: $(curl --version | head -1)"
  echo "# date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# prefix: $PREFIX/"
  cat "$TMP/results"
} > "$OUT"
cat "$OUT"
exit $FAILED
