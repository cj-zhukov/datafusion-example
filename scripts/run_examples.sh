#!/usr/bin/env bash

set -euo pipefail

EXAMPLES_DIR="examples"

for file in "$EXAMPLES_DIR"/*.rs; do
    example_name=$(basename "$file" .rs)

    echo "Running example: $example_name"
    echo "----------------------------------------"

    cargo run --example "$example_name"

    echo
done
