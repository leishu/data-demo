#!/usr/bin/env bash
# 构建 Vue 控制台：产物直接写进 lake-service/src/main/resources/static/ui
#   bin/build-ui.sh            npm install（缺 node_modules 时）+ vite build
#   bin/build-ui.sh --clean    先清 node_modules 再装
set -euo pipefail
cd "$(dirname "$0")/.."

UI_DIR="lake-ui"

find_node() {
  if command -v node >/dev/null 2>&1; then
    command -v node; return 0
  fi
  local cand
  for cand in "${HOME}"/.workbuddy/binaries/node/versions/*/bin/node /usr/local/bin/node; do
    [[ -x "${cand}" ]] && { echo "${cand}"; return 0; }
  done
  return 1
}

NODE_BIN="$(find_node)" || {
  echo "找不到 node（需要 20.19+ / 22.12+）：装一个或把 node 加进 PATH 后重试" >&2
  exit 1
}
NODE_DIR="$(dirname "${NODE_BIN}")"
echo "使用 node: $("${NODE_BIN}" -v) (${NODE_BIN})"

cd "${UI_DIR}"

if [[ "${1:-}" == "--clean" ]]; then
  rm -rf node_modules
fi

if [[ ! -d node_modules ]]; then
  # 默认走官方源；国内网络慢时可以 NPM_REGISTRY=https://registry.npmmirror.com bin/build-ui.sh
  "${NODE_DIR}/npm" install \
    --registry="${NPM_REGISTRY:-https://registry.npmjs.org}" \
    --no-audit --no-fund
fi

"${NODE_DIR}/npm" run build

echo
echo "产物：lake-service/src/main/resources/static/ui"
ls -1 ../lake-service/src/main/resources/static/ui
