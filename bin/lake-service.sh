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
PID_FILE="logs/lake-service.pid"

# 统一用绝对路径：pid 文件与兜底 pkill 都靠它精确定位本仓库的实例，避免误杀机器上同名进程
abs_jar() {
  (cd "$(dirname "$1")" && printf '%s/%s' "$(pwd)" "$(basename "$1")")
}

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
        echo "前端构建失败：jar 里不会带控制台，已中止构建（单独跑 bin/build-ui.sh 看原因）" >&2
        exit 1
      fi
    fi
    export JAVA_HOME="${JAVA21_HOME}"
    export PATH="${JAVA21_HOME}/bin:${PATH}"
    mvn -q -DskipTests -pl lake-service -am package
    # 同 bin/build.sh：给 dev-batch.sh 预生成运行时 classpath，跑作业时不再 mvn package
    if [[ -d spark-batch/target/classes ]]; then
      mvn -q -DskipTests -Ps3 -pl spark-batch -am dependency:build-classpath \
        -Dmdep.outputFile="${PWD}/spark-batch/target/classpath.txt"
    fi
    echo "已生成 ${JAR_PATTERN}"
    ;;

  run)
    JAVA21_HOME="$(find_jdk21)" || { echo "需要 JDK 21：设 JAVA21_HOME 指到 jdk-21 目录" >&2; exit 1; }
    jar="$(ls -1 ${JAR_PATTERN} 2>/dev/null | grep -v sources | head -1)"
    [[ -n "${jar}" ]] || { echo "先执行 bin/lake-service.sh build" >&2; exit 1; }
    exec "${JAVA21_HOME}/bin/java" -jar "$(abs_jar "${jar}")" --server.port="${PORT}"
    ;;

  start)
    JAVA21_HOME="$(find_jdk21)" || { echo "需要 JDK 21：设 JAVA21_HOME 指到 jdk-21 目录" >&2; exit 1; }
    jar="$(ls -1 ${JAR_PATTERN} 2>/dev/null | grep -v sources | head -1)"
    [[ -n "${jar}" ]] || { echo "先执行 bin/lake-service.sh build" >&2; exit 1; }
    mkdir -p logs
    nohup "${JAVA21_HOME}/bin/java" -jar "$(abs_jar "${jar}")" --server.port="${PORT}" \
      > logs/lake-service.out 2>&1 &
    echo "$!" > "${PID_FILE}"
    echo "pid=$! port=${PORT} log=logs/lake-service.out"
    ;;

  stop)
    stopped=0
    if [[ -f "${PID_FILE}" ]]; then
      pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
      if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        # 先 SIGTERM 让 Spring 优雅关闭，最多等 10s，再兜底 SIGKILL
        kill "${pid}" 2>/dev/null && stopped=1
        for _ in {1..50}; do
          kill -0 "${pid}" 2>/dev/null || break
          sleep 0.2
        done
        kill -0 "${pid}" 2>/dev/null && kill -9 "${pid}" 2>/dev/null || true
      fi
      rm -f "${PID_FILE}"
    fi
    if [[ "${stopped}" == "0" ]]; then
      # 兼容没有 pid 文件的旧实例：只匹配本仓库 target 下 jar 的绝对路径
      jar="$(ls -1 ${JAR_PATTERN} 2>/dev/null | grep -v sources | head -1)"
      if [[ -n "${jar}" ]] && pkill -f "$(abs_jar "${jar}")"; then
        stopped=1
      fi
    fi
    [[ "${stopped}" == "1" ]] && echo "已停止" || echo "没有在跑的实例"
    ;;

  *)
    echo "用法: bin/lake-service.sh {build|run|start|stop}" >&2
    exit 2
    ;;
esac
