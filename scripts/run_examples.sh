#!/usr/bin/env bash

cd examples/
SKIP_LIST=()

skip_example() {
    local name="$1"
    for skip in "${SKIP_LIST[@]}"; do
        if [ "$name" = "$skip" ]; then
            return 0
        fi
    done
    return 1
}

for dir in */; do
    example_name=$(basename "$dir")

    if skip_example "$example_name"; then
        echo "Skipping $example_name"
        continue
    fi

    echo "Running example group: $example_name"
    cargo run --example "$example_name" -- all
done
