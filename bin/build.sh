#!/usr/bin/env bash
# 全量构建：整个 reactor 要用 JDK 21（lake-service 编译目标是 21，其余模块编译目标仍是 11）
#   bin/build.sh                     打包（含测试）
#   bin/build.sh -DskipTests         跳过测试
#   bin/build.sh -pl spark-batch -am 只构建单个模块（这种用法 JDK 11 也能跑）
set -euo pipefail
cd "$(dirname "$0")/.."

find_jdk21() {
  if [[ -n "${JAVA21_HOME:-}" && -x "${JAVA21_HOME}/bin/java" ]]; then
    echo "${JAVA21_HOME}"; return 0
  fi
  local cand
  for cand in "${HOME}"/soft/jdk-21* /usr/lib/jvm/java-21-openjdk-*; do
    if [[ -x "${cand}/bin/java" ]] && "${cand}/bin/java" -version 2>&1 | grep -q '"21'; then
      echo "${cand}"; return 0
    fi
  done
  return 1
}

JAVA21="$(find_jdk21)" || {
  echo "找不到 JDK 21：export JAVA21_HOME=/path/to/jdk-21 后重试" >&2
  exit 1
}

echo "使用 JDK: ${JAVA21}"
REPO_ARGS=()
[[ -d "${HOME}/.m2/repository" ]] && REPO_ARGS+=("-Dmaven.repo.local=${HOME}/.m2/repository")

# 前端产物要先于 mvn package 生成，否则 jar 里没有控制台页面。
# 只构建 spark 模块、或者显式 SKIP_UI=1 时跳过，省掉一次 npm。
MVN_ARGS=("${@:-package}")
if [[ "${SKIP_UI:-0}" != "1" ]] && [[ "${MVN_ARGS[*]}" != *"-pl spark-batch"* && "${MVN_ARGS[*]}" != *"-pl platform-common"* ]]; then
  if command -v node >/dev/null 2>&1 || ls "${HOME}"/.workbuddy/binaries/node/versions/*/bin/node >/dev/null 2>&1; then
    echo "构建前端 lake-ui ..."
    if ! bin/build-ui.sh >"${TMPDIR:-/tmp}/lake-ui-build.log" 2>&1; then
      echo "警告：前端构建失败（jar 里不会带控制台，接口不受影响），日志：" >&2
      tail -20 "${TMPDIR:-/tmp}/lake-ui-build.log" >&2
    fi
  else
    echo "跳过前端：找不到 node（需要时单独跑 bin/build-ui.sh）"
  fi
fi

JAVA_HOME="${JAVA21}" PATH="${JAVA21}/bin:${PATH}" \
  exec mvn -B "${REPO_ARGS[@]}" "${MVN_ARGS[@]}"
