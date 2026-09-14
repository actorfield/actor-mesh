#!/bin/bash
# build.sh — cross-platform build for actor mesh
#
# Verified targets (works from Linux host):
#   ./build.sh                                 native
#   ./build.sh --target x86_64-linux-musl     ✓ static Intel binary
#   ./build.sh --target aarch64-linux-musl    ✓ static ARM64 binary
#
# Needs target device (seconds to compile):
#   macOS:   brew install nng lmdb && make
#   Windows: MSYS2 + pacman -S mingw-w64-x86_64-nng mingw-w64-x86_64-lmdb
#            then make CC=gcc
#
# Requires: zig (https://ziglang.org/download)
# Zig bundles clang + libc headers. Linux targets need nothing else.

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
TARGET="native"
case "${1:-}" in
    --target) TARGET="$2"; shift 2 ;;
    --target=*) TARGET="${1#*=}" ;;
    native|*-*-*) TARGET="$1" ;;
esac
ZIG="${ZIG:-zig}"

# --- config ---
NNG_VERSION="1.9.0"
LMDB_VERSION="0.9.33"

CFLAGS="-Wall -Wextra -O2 -std=c11 -I$ROOT/runtime"
BUILD_DIR="$ROOT/build/$TARGET"
DEPS_DIR="$BUILD_DIR/deps"

if [ "$TARGET" != "native" ]; then
    CFLAGS="$CFLAGS -target $TARGET"
fi

# Binary extension
EXE=""
case "$TARGET" in *windows*) EXE=".exe" ;; esac

echo "=== actor mesh build ==="
echo "  target: $TARGET"
echo "  zig:    $($ZIG version)"
echo "  cc:     $ZIG cc"

mkdir -p "$BUILD_DIR" "$DEPS_DIR"

# --- lmdb (single C file, trivial to cross-compile) ---
LMDB_SRC="$ROOT/vendor/lmdb"
if [ ! -f "$DEPS_DIR/liblmdb.a" ]; then
    echo "=== building lmdb ==="
    mkdir -p "$LMDB_SRC"
    if [ ! -f "$LMDB_SRC/lmdb.h" ]; then
        # fetch lmdb source
        curl -sL "https://github.com/LMDB/lmdb/archive/refs/tags/LMDB_${LMDB_VERSION}.tar.gz" \
            | tar xz -C /tmp
        cp /tmp/lmdb-LMDB_${LMDB_VERSION}/libraries/liblmdb/lmdb.h "$LMDB_SRC/"
        cp /tmp/lmdb-LMDB_${LMDB_VERSION}/libraries/liblmdb/mdb.c  "$LMDB_SRC/"
        cp /tmp/lmdb-LMDB_${LMDB_VERSION}/libraries/liblmdb/midl.c "$LMDB_SRC/"
        cp /tmp/lmdb-LMDB_${LMDB_VERSION}/libraries/liblmdb/midl.h "$LMDB_SRC/"
        rm -rf /tmp/lmdb-LMDB_${LMDB_VERSION}
    fi
    $ZIG cc $CFLAGS -c "$LMDB_SRC/mdb.c"  -o "$DEPS_DIR/mdb.o"
    $ZIG cc $CFLAGS -c "$LMDB_SRC/midl.c" -o "$DEPS_DIR/midl.o"
    $ZIG ar crs "$DEPS_DIR/liblmdb.a" "$DEPS_DIR/mdb.o" "$DEPS_DIR/midl.o"
    echo "  -> $DEPS_DIR/liblmdb.a"
fi

# --- nng (needs cmake + zig for cross-compile) ---
NNG_SRC="$ROOT/vendor/nng"
NNG_BUILD="$BUILD_DIR/nng-build"
if [ ! -f "$DEPS_DIR/libnng.a" ]; then
    echo "=== building nng ==="
    if [ ! -f "$NNG_SRC/CMakeLists.txt" ]; then
        git clone --depth 1 --branch "v$NNG_VERSION" \
            https://github.com/nanomsg/nng.git "$NNG_SRC" 2>/dev/null || true
    fi
    mkdir -p "$NNG_BUILD"
    cd "$NNG_BUILD"
    export CC="$ZIG cc"
    export AR="$ZIG ar"
    export RANLIB="$ZIG ranlib"
    CFLAGS_CMAKE=""
    CMAKE_SYSTEM=""
    case "$TARGET" in
        *windows*) CMAKE_SYSTEM="-DCMAKE_SYSTEM_NAME=Windows" ;;
        *macos*)   CMAKE_SYSTEM="-DCMAKE_SYSTEM_NAME=Darwin" ;;
        *linux*)   CMAKE_SYSTEM="-DCMAKE_SYSTEM_NAME=Linux" ;;
    esac
    [ "$TARGET" != "native" ] && CFLAGS_CMAKE="-target $TARGET"
    cmake "$NNG_SRC" \
        $CMAKE_SYSTEM \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DNNG_PROTOCOL_PAIR0=OFF \
        -DNNG_PROTOCOL_REQ0=OFF \
        -DNNG_PROTOCOL_REP0=OFF \
        -DNNG_PROTOCOL_PUSH0=OFF \
        -DNNG_PROTOCOL_PULL0=OFF \
        -DNNG_PROTOCOL_SURVEYOR0=OFF \
        -DNNG_PROTOCOL_RESPONDENT0=OFF \
        -DNNG_PROTOCOL_BUS0=OFF \
        -DNNG_TRANSPORT_INPROC=OFF \
        -DNNG_TRANSPORT_IPC=OFF \
        -DNNG_TRANSPORT_WS=OFF \
        -DNNG_TRANSPORT_WSS=OFF \
        -DNNG_TRANSPORT_ZEROTIER=OFF \
        -DNNG_ENABLE_TLS=OFF \
        -DNNG_TOOLS=OFF \
        -DNNG_TESTS=OFF \
        -DCMAKE_C_FLAGS="$CFLAGS_CMAKE"
    make -j$(nproc) nng
    cp libnng.a "$DEPS_DIR/"
    echo "  -> $DEPS_DIR/libnng.a"
    cd "$ROOT"
fi

# --- actor mesh ---
echo "=== building actor mesh ==="
INCLUDES="-I$NNG_SRC/include -I$LMDB_SRC"
LIBS="$DEPS_DIR/libnng.a $DEPS_DIR/liblmdb.a -lpthread"
LDFLAGS=""

# static linking for musl Linux
case "$TARGET" in
    *musl*) LDFLAGS="$LDFLAGS -static" ;;
esac

# actor.c calls the isolation unit on every target but Windows
ISOLATION="$ROOT/runtime/actor_isolation.c"
case "$TARGET" in *windows*) ISOLATION="" ;; esac

$ZIG cc $CFLAGS $LDFLAGS $INCLUDES \
    "$ROOT/runtime/main.c" "$ROOT/runtime/actor.c" "$ROOT/runtime/bus.c" $ISOLATION \
    $LIBS -o "$BUILD_DIR/actor$EXE"

$ZIG cc $CFLAGS $LDFLAGS $INCLUDES \
    "$ROOT/proxy/proxy.c" "$ROOT/runtime/bus.c" \
    $LIBS -o "$BUILD_DIR/mesh-proxy$EXE"

echo ""
echo "=== done ==="
ls -lh "$BUILD_DIR/actor$EXE" "$BUILD_DIR/mesh-proxy$EXE"
file "$BUILD_DIR/actor$EXE"
