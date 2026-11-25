#!/usr/bin/env bash
# Builds the .deb and .rpm for a Linux target from an existing release build.
#
# Usage: scripts/package.sh <target>   (e.g. x86_64-unknown-linux-gnu)
#
# Needs cargo-deb and cargo-generate-rpm, and target/<target>/release/ferrited already built.
# The package layout is in Cargo.toml under [package.metadata.deb] and
# [package.metadata.generate-rpm]. Prints the paths of the two packages.
set -euo pipefail

target=${1:?usage: scripts/package.sh <target>}
cd "$(dirname "$0")/.."

# deploy/ferrited.service is written for a binary installed by hand in /usr/local/bin.
mkdir -p target/pkg
sed -e 's|/usr/local/bin/ferrited|/usr/bin/ferrited|g' \
	-e 's|Install as /etc/systemd/system/ferrited.service|Installed by the package as /usr/lib/systemd/system/ferrited.service|' \
	deploy/ferrited.service > target/pkg/ferrited.service
grep -q '^ExecStart=/usr/bin/ferrited ' target/pkg/ferrited.service

cargo deb --no-build --no-strip --target "$target" >&2
cargo generate-rpm --target "$target" >&2

ls "target/$target"/debian/*.deb "target/$target"/generate-rpm/*.rpm
