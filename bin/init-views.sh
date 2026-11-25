#!/usr/bin/env bash
# 在 Trino 侧创建/刷新脱敏视图（lake-adhoc）：java -cp 直起，依赖 bin/build.sh 的产物
#
#   bin/init-views.sh --print-only                  # 只打印渲染后的 SQL
#   bin/init-views.sh --user etl                    # security profile 下用有 OWNERSHIP 的账号
set -euo pipefail
cd "$(dirname "$0")/.."

CLASSES="lake-adhoc/target/classes"
COMMON_CLASSES="platform-common/target/classes"
CLASSPATH_FILE="lake-adhoc/target/classpath.txt"

if [[ ! -d "${CLASSES}" || ! -d "${COMMON_CLASSES}" || ! -s "${CLASSPATH_FILE}" ]]; then
  echo "缺少预构建产物（${CLASSES} / ${COMMON_CLASSES} / ${CLASSPATH_FILE}）" >&2
  echo "请先执行一次 bin/build.sh" >&2
  exit 2
fi

JAVA_BIN=""
if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  JAVA_BIN="${JAVA_HOME}/bin/java"
elif command -v java >/dev/null 2>&1; then
  JAVA_BIN="$(command -v java)"
fi
if [[ -z "${JAVA_BIN}" ]]; then
  echo "找不到 java（需要 JDK 17+）：设置 JAVA_HOME 或把 java 放进 PATH" >&2
  exit 2
fi

exec "${JAVA_BIN}" -cp "${CLASSES}:${COMMON_CLASSES}:$(cat "${CLASSPATH_FILE}")" \
  com.demo.lake.adhoc.TrinoViews "$@"
