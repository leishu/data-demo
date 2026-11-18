#!/usr/bin/env bash
# lake-service 启停脚本（Spring Boot 4 需要 JDK 21；其余模块仍用 JDK 11 构建/运行）
#   bin/lake-service.sh build          打包
#   bin/lake-service.sh run            前台启动（Ctrl+C 退出）
#   bin/lake-service.sh start          后台启动，日志 logs/lake-service.out
#   bin/lake-service.sh stop           停止
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-8088}"
JAR_PATTERN="lake-service/target/lake-service-*.jar"

find_jdk21() {
  if [[ -n "${JAVA21_HOME:-}" && -x "${JAVA21_HOME}/bin/java" ]]; then
    echo "${JAVA21_HOME}"; return 0
  fi
  local cand
  for cand in "${HOME}"/soft/jdk-21* "${HOME}"/soft/jdk21* /usr/lib/jvm/java-21-openjdk-*; do
    if [[ -x "${cand}/bin/java" ]] && "${cand}/bin/java" -version 2>&1 | grep -q '"21'; then
      echo "${cand}"; return 0
    fi
  done
  return 1
}

cmd="${1:-run}"

case "${cmd}" in
  build)
    JAVA21_HOME="$(find_jdk21)" || { echo "需要 JDK 21：设 JAVA21_HOME 指到 jdk-21 目录" >&2; exit 1; }
    # 控制台页面要先进 jar；没有 node 时降级为「只有接口，没有页面」
    if [[ "${SKIP_UI:-0}" != "1" ]]; then
      if ! bin/build-ui.sh >/dev/null 2>&1; then
        echo "警告：前端构建失败，jar 里不会带控制台（单独跑 bin/build-ui.sh 看原因）" >&2
      fi
    fi
    JAVA_HOME="${JAVA21_HOME}" PATH="${JAVA21_HOME}/bin:${PATH}" \
      mvn -q -DskipTests -pl lake-service -am package
    echo "已生成 ${JAR_PATTERN}"
    ;;

  run)
    JAVA21_HOME="$(find_jdk21)" || { echo "需要 JDK 21：设 JAVA21_HOME 指到 jdk-21 目录" >&2; exit 1; }
    jar="$(ls -1 ${JAR_PATTERN} 2>/dev/null | grep -v sources | head -1)"
    [[ -n "${jar}" ]] || { echo "先执行 bin/lake-service.sh build" >&2; exit 1; }
    exec "${JAVA21_HOME}/bin/java" -jar "${jar}" --server.port="${PORT}"
    ;;

  start)
    JAVA21_HOME="$(find_jdk21)" || { echo "需要 JDK 21：设 JAVA21_HOME 指到 jdk-21 目录" >&2; exit 1; }
    jar="$(ls -1 ${JAR_PATTERN} 2>/dev/null | grep -v sources | head -1)"
    [[ -n "${jar}" ]] || { echo "先执行 bin/lake-service.sh build" >&2; exit 1; }
    mkdir -p logs
    nohup "${JAVA21_HOME}/bin/java" -jar "${jar}" --server.port="${PORT}" \
      > logs/lake-service.out 2>&1 &
    echo "pid=$! port=${PORT} log=logs/lake-service.out"
    ;;

  stop)
    pkill -f "lake-service-.*\.jar" && echo "已停止" || echo "没有在跑的实例"
    ;;

  *)
    echo "用法: bin/lake-service.sh {build|run|start|stop}" >&2
    exit 2
    ;;
esac
