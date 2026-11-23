#!/usr/bin/env bash
# 全量构建：整个 reactor 要用 JDK 21（lake-service 编译目标是 21，其余模块编译目标仍是 11）
#   bin/build.sh                     打包（含测试）+ 预生成 spark-batch / lake-adhoc 运行时 classpath
#   bin/build.sh -DskipTests         跳过测试
#   bin/build.sh -pl spark-batch -am 只构建单个模块（这种用法 JDK 11 也能跑）
set -euo pipefail
cd "$(dirname "$0")/.."

find_jdk21() {
  if [[ -n "${JAVA21_HOME:-}" && -x "${JAVA21_HOME}/bin/java" ]]; then
    echo "${JAVA21_HOME}"; return 0
  fi
  # CI / asdf / SDKMAN 通常只设 JAVA_HOME；只有确实是 21 才采用，避免把 JDK 11 当 21 用
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]] \
      && "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q '"21'; then
    echo "${JAVA_HOME}"; return 0
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

# 只传 -DskipTests / -pl / -am 这类参数时，mvn 会报 "No goals have been specified"。
# 没显式给生命周期/插件目标就补一个 package（参数以 - 开头的都视为选项）。
MVN_ARGS=("$@")
if [[ ${#MVN_ARGS[@]} -eq 0 ]]; then
  MVN_ARGS=(package)
else
  # 注意 -pl spark-batch 里的 spark-batch 是选项值，不是生命周期目标
  HAS_GOAL=0
  EXPECT_VALUE=0
  for arg in "${MVN_ARGS[@]}"; do
    if [[ "${EXPECT_VALUE}" -eq 1 ]]; then
      EXPECT_VALUE=0
      continue
    fi
    case "$arg" in
      -pl|--projects|-P|--activate-profiles|-f|--file|-s|--settings|-gs|--global-settings|\
      -T|--threads|-rf|--resume-from|-l|--log-file)
        EXPECT_VALUE=1 ;;
      -*) ;;
      *) HAS_GOAL=1 ;;
    esac
  done
  [[ "${HAS_GOAL}" -eq 1 ]] || MVN_ARGS+=(package)
fi

# 前端产物要先于 mvn package 生成，否则 jar 里没有控制台页面。
# 只构建 spark 模块、或者显式 SKIP_UI=1 时跳过，省掉一次 npm。
if [[ "${SKIP_UI:-0}" != "1" ]] && [[ "${MVN_ARGS[*]}" != *"-pl spark-batch"* && "${MVN_ARGS[*]}" != *"-pl platform-common"* ]]; then
  if command -v node >/dev/null 2>&1 || ls "${HOME}"/.workbuddy/binaries/node/versions/*/bin/node >/dev/null 2>&1; then
    echo "构建前端 lake-ui ..."
    if ! bin/build-ui.sh >"${TMPDIR:-/tmp}/lake-ui-build.log" 2>&1; then
      echo "前端构建失败：jar 里不会带控制台，已中止构建（单独跑 bin/build-ui.sh 看原因），日志：" >&2
      tail -20 "${TMPDIR:-/tmp}/lake-ui-build.log" >&2
      exit 1
    fi
  else
    echo "跳过前端：找不到 node（需要时单独跑 bin/build-ui.sh）"
  fi
fi

export JAVA_HOME="${JAVA21}"
export PATH="${JAVA21}/bin:${PATH}"

mvn -B "${REPO_ARGS[@]}" "${MVN_ARGS[@]}"

# 预生成 spark-batch 运行时 classpath：dev-batch.sh 用 java -cp 直接起批处理，
# 不再每次提交作业都跑一遍 mvn package（并发作业会抢同一个 target 目录）。
# 带 -Ps3 一次把 S3 依赖也放进去，运行期按 warehouse 自动选，无需再切 profile。
if [[ -d spark-batch/target/classes ]]; then
  echo "生成 spark-batch 运行时 classpath ..."
  mvn -q -B "${REPO_ARGS[@]}" -Ps3 -pl spark-batch -am \
    dependency:build-classpath \
    -Dmdep.outputFile="${PWD}/spark-batch/target/classpath.txt"
fi

# 预生成 lake-adhoc 运行时 classpath：trino-query.sh 用 java -cp 直起，
# 不再每次查询都付一遍 Maven + JVM 启动开销（SQL 参数也就不用再绕 exec.args 分词）。
if [[ -d lake-adhoc/target/classes ]]; then
  echo "生成 lake-adhoc 运行时 classpath ..."
  # 固定插件版本，避免不同 Maven 默认版本（2.8 / 3.x）行为漂移；-DincludeScope=runtime 两者都支持
  mvn -q -B "${REPO_ARGS[@]}" -pl lake-adhoc -am \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
    -DincludeScope=runtime \
    -Dmdep.outputFile=target/classpath.txt
fi
