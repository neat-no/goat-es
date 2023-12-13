#!/bin/bash
set -e

rootdir="$(pwd)"
bindir="${rootdir}/internal/.bin"
checkdir="${rootdir}/internal/sha"

check_bin() {
	local name="$1"
	local check="${checkdir}/${name}.sha1sum"
	[ -f "${check}" ] &&
	sha1sum --status --check "${check}"
	# sha1sum "${bindir}/${name}"
}

install_buf() {
	mkdir -p "${bindir}"

	BIN="${bindir}" && \
	VERSION="1.28.1" && \
	echo "Downloading buf ${VERSION}..." && \
	curl -sSL \
	"https://github.com/bufbuild/buf/releases/download/v${VERSION}/buf-$(uname -s)-$(uname -m)" \
	-o "${BIN}/buf" && \
	chmod +x "${BIN}/buf"
}

check_bin "buf" || install_buf
