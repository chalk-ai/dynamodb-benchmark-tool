#!/bin/bash
set -e

echo "Cross-compiling dynamodbbench for x86_64 Linux..."

# Check if musl toolchain is installed
if ! command -v x86_64-linux-musl-gcc &> /dev/null; then
    echo "Error: x86_64-linux-musl-gcc not found."
    echo "Please install the toolchain with:"
    echo "    brew install FiloSottile/musl-cross/musl-cross"
    exit 1
fi

# Check if rustup target is installed
if ! rustup target list | grep -q "x86_64-unknown-linux-musl (installed)"; then
    echo "Installing x86_64-unknown-linux-musl target..."
    rustup target add x86_64-unknown-linux-musl
fi

# Build the binary
echo "Building for x86_64-unknown-linux-musl..."
cargo build --release --target x86_64-unknown-linux-musl

echo "Build complete!"
echo "Your binary is at: target/x86_64-unknown-linux-musl/release/dynamodbbench"
echo
echo "To deploy to EC2, use:"
echo "scp -i your-key.pem target/x86_64-unknown-linux-musl/release/dynamodbbench ec2-user@your-ec2-instance:~/"