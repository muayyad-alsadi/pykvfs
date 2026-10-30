#!/usr/bin/env bash
# Filesystem-level benchmark: extract and re-archive a real tree through a mount.
#
# Usage: FORMAT=tar|tgz fs-bench.sh LABEL MOUNT VERIFY_MOUNT [RUNS]
#   FORMAT        tar  = plain, uncompressed archive (default; isolates the mount)
#                 tgz  = gzip -3 compressed archive (adds compression CPU time)
#   MOUNT         where the tree is extracted and archived (the mount under test)
#   VERIFY_MOUNT  a second mount of the same namespace; the extracted tree is read back
#                 through it and compared with a reference manifest (cross-mount check)
#
# Prints CSV lines: label,step,run,seconds. Verification results go to stderr.
set -euo pipefail

ARCHIVE_TGZ=/home/alsadi/Downloads/photocraft-0.7.0-linux-x86_64.tar.gz
TOP=photocraft-0.7.0-linux-x86_64
REF=/tmp/fs-ref
FORMAT=${FORMAT:-tar}
LABEL=$1
MOUNT=$2
VERIFY=$3
RUNS=${4:-3}

case "$FORMAT" in
  tar) SOURCE=$REF/source.tar; OUT=/tmp/delme.tar; EXTRACT=(tar -xf); ARCHIVE=(tar -cf) ;;
  tgz) SOURCE=$ARCHIVE_TGZ; OUT=/tmp/delme.tgz; EXTRACT=(tar -xzf); ARCHIVE=(tar -I "gzip -3" -cf) ;;
  *) echo "unknown FORMAT=$FORMAT" >&2; exit 2 ;;
esac

if [[ ! -f "$REF/manifest.txt" ]]; then
  mkdir -p "$REF"
  tar -xzf "$ARCHIVE_TGZ" -C "$REF"
  (cd "$REF/$TOP" && find . -type f -exec sha256sum {} + | sort -k2) > "$REF/manifest.txt"
fi
if [[ ! -f "$REF/source.tar" ]]; then
  tar -cf "$REF/source.tar" -C "$REF" "$TOP"
fi

now() { date +%s.%N; }

# timed STEP RUN COMMAND...  prints the wall-clock seconds of COMMAND
timed() {
  local step=$1 run=$2
  shift 2
  local t0 t1
  t0=$(now)
  "$@"
  t1=$(now)
  awk -v l="$LABEL" -v s="$step" -v r="$run" -v a="$t0" -v b="$t1" \
    'BEGIN { printf "%s,%s,%s,%.3f\n", l, s, r, b - a }'
}

verify() {
  local run=$1
  (cd "$VERIFY/$TOP" && find . -type f -exec sha256sum {} + | sort -k2) > "/tmp/fs-manifest-$LABEL.txt"
  if diff -q "$REF/manifest.txt" "/tmp/fs-manifest-$LABEL.txt" > /dev/null; then
    echo "verify run=$run: PASS ($(wc -l < "$REF/manifest.txt") files match the reference)" >&2
  else
    echo "verify run=$run: FAIL, see diff of $REF/manifest.txt and /tmp/fs-manifest-$LABEL.txt" >&2
  fi
}

for run in $(seq 1 "$RUNS"); do
  rm -rf "${MOUNT:?}/$TOP"
  timed extract "$run" "${EXTRACT[@]}" "$SOURCE" -C "$MOUNT"
  verify "$run"
  timed find_stat "$run" sh -c "find '$MOUNT/$TOP' -printf '%s %m %u %T@\n' > /dev/null"
  rm -f "$OUT"
  timed archive "$run" "${ARCHIVE[@]}" "$OUT" -C "$MOUNT" "$TOP"
done
# The last archive and extraction stay in place for inspection.
