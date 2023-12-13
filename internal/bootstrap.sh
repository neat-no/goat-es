#!/bin/bash
set -e

rootdir="$(pwd)"
bindir="${rootdir}/internal/.bin"
checkdir="${rootdir}/internal/sha"
dprint_dir="${rootdir}/internal/dprint"

check_bin() {
	local name="$1"
	local check="${checkdir}/${name}.sha1sum"
	[ -f "${check}" ] && sha1sum --status --check "${check}"
}

check_dprint() {
	local name="$1"
	local check="${checkdir}/${name}.sha1sum"

}

install_buf() {
	mkdir -p "${bindir}"

	check_bin "buf" && return

	BIN="${bindir}" && \
	VERSION="1.28.1" && \
	echo "Downloading buf ${VERSION}..." && \
	curl -sSL \
	"https://github.com/bufbuild/buf/releases/download/v${VERSION}/buf-$(uname -s)-$(uname -m)" \
	-o "${BIN}/buf" && \
	chmod +x "${BIN}/buf"
}

install_dprint_plugin() {
	local name="$1"
	local plugin="${dprint_dir}/${name}"

	check_bin "${name}" && return
	mkdir -p "${dprint_dir}"
	curl -fsSL "https://plugins.dprint.dev/${name}" --output "${plugin}"
}

install_buf
install_dprint_plugin "typescript-0.88.1.wasm"
install_dprint_plugin "json-0.17.4.wasm"
