#!/bin/sh
# gen-compile-flags.sh — regenerate compile_flags.txt from pkg-config.
#
# clangd (which Zed uses for C/C++) does not read the Makefile and cannot call
# pkg-config itself, so its view of the include paths is materialised into
# compile_flags.txt at the repo root. Run this after changing dependencies or
# moving to a machine with a different Homebrew prefix.
#
# Usage:  sh tools/gen-compile-flags.sh

set -e

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# Mirror the Makefile: Homebrew kegs on the pkg-config path on macOS only.
case "$(uname -s)" in
  Darwin)
    brew_prefix="$(brew --prefix)"
    PKG_CONFIG_PATH="$root/pkgconfig:$brew_prefix/opt/lmdb/lib/pkgconfig:$brew_prefix/lib/pkgconfig"
    export PKG_CONFIG_PATH
    ;;
esac

out="$root/compile_flags.txt"

{
    # Project headers (actor_tuple.h, bus.h, ...) live in runtime/.
    echo -Iruntime
    pkg-config --cflags nng  2>/dev/null || true
    pkg-config --cflags lmdb 2>/dev/null || true
} | tr ' ' '\n' | grep -v '^$' > "$out"

echo "wrote $out:"
sed 's/^/  /' "$out"
