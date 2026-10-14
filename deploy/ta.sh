#!/usr/bin/env bash
# 固定使用当前机器的默认构建器，避免误用遗留 ARM 构建器。
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export BUILDX_BUILDER=default
exec docker compose "$@"
