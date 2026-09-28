#!/bin/sh -e
# Builds the read-scaling fanout image from taurionui at a PINNED commit, unmodified
# (owner ruling 2026-09-28). Run on the laptop; the image is local like tn-base.
REV="${1:-e8f45c6}"
DIR="${TMPDIR:-/tmp}/taurionui-$REV"
if [ ! -d "$DIR" ]; then git clone https://github.com/xaya/taurionui.git "$DIR"; fi
cd "$DIR"
git fetch -q origin
git checkout -q "$REV"
docker build -t "tn-fanout:$REV" .
echo "built tn-fanout:$REV"
