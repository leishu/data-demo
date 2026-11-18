#!/usr/bin/env bash
# 一次性装好 Docker Engine + Compose v2（Ubuntu 22.04/24.04、Debian 12）
#
#   bash bin/install-docker.sh
#
# 为什么用脚本而不是 apt install docker.io：
#   - docker.io 是发行版打包的旧版本，不带 compose v2 / buildx 插件；
#   - 后面要拉 iceberg / hive / presto 这些多架构镜像，buildx 基本是刚需。
#
# 可覆盖的环境变量：
#   DOCKER_APT_MIRROR        apt 源列表，逗号分隔，默认阿里云 + 清华（本机到
#                            download.docker.com 的 TLS 会被重置，官方源用不了）
#   DOCKER_REGISTRY_MIRRORS  镜像加速器，逗号分隔
#   DOCKER_USER              要加入 docker 组的用户（默认取 sudo 发起人）
#   DOCKER_SKIP_PULL_TEST=1  跳过最后的 hello-world 拉取测试
#   DOCKER_SKIP_PROXIES=1    不把当前 shell 的代理写进 dockerd 配置
#
# 装完需要重新登录（或 newgrp docker）才能免 sudo 使用 docker。
set -euo pipefail

log()  { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

SUDO=""
if [[ ${EUID} -ne 0 ]]; then
  command -v sudo >/dev/null || die "需要 root 或 sudo 权限"
  SUDO="sudo"
fi
run() { if [[ -n "${SUDO}" ]]; then sudo "$@"; else "$@"; fi; }

# ---------------------------------------------------------------- 发行版检查
[[ -r /etc/os-release ]] || die "读不到 /etc/os-release"
# shellcheck disable=SC1091
. /etc/os-release
case "${ID:-}" in
  ubuntu|debian) ;;
  *) die "只支持 Ubuntu/Debian，当前是 ${ID:-unknown}" ;;
esac
CODENAME="${VERSION_CODENAME:-}"
[[ -n "${CODENAME}" ]] || die "识别不到 VERSION_CODENAME"
ARCH="$(dpkg --print-architecture)"
log "发行版 ${PRETTY_NAME} / ${ARCH}"

# ---------------------------------------------------------------- 基础依赖
log "安装基础依赖 (ca-certificates / curl / gnupg / python3)"
run apt-get update -qq
run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ca-certificates curl gnupg python3

# 发行版自带的 docker.io / containerd 会和 docker-ce 抢同名文件
if dpkg -s docker.io >/dev/null 2>&1; then
  warn "检测到 docker.io，先移除以免和 docker-ce 冲突"
  run env DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq \
    docker.io docker-doc docker-compose podman-docker containerd runc || true
fi

APT_PKGS=(docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin)

apt_install() {
  # -y 免确认；不带 --no-install-recommends：buildx / compose 插件依赖推荐包，
  # 少了推荐包可能出现 docker build / docker compose 不可用，省这几十 MB 不值当。
  run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${APT_PKGS[@]}"
}

# ---------------------------------------------------------------- apt 源
# 本机到 download.docker.com 的 TLS 会被重置，所以只能用国内镜像源；
# 按顺序逐个试，全部拿不到 GPG 才报错退出。
MIRRORS_CSV="${DOCKER_APT_MIRROR:-https://mirrors.aliyun.com/docker-ce,https://mirrors.tuna.tsinghua.edu.cn/docker-ce}"
IFS=',' read -r -a APT_MIRRORS <<<"${MIRRORS_CSV}"
run install -m 0755 -d /etc/apt/keyrings

fetch_gpg() {
  # 两种格式（.gpg 与 .asc）都试；先下载到临时文件，成功后再改名，
  # 避免 curl 失败时留下半截文件被 signed-by 引用。
  # 注意：这两个地址回的都是 ASCII armor 文本，而 apt（22.04/24.04 走 gpgv）
  # 只认二进制 keyring。不 dearmor 会报“仓库 ... InRelease 没有数字签名”，
  # 真实原因是 gpgv 读不出公钥，apt 把 NO_PUBKEY 包装成了这句话。
  local base="$1" out="" f tmp raw
  tmp="/etc/apt/keyrings/.docker-dl.$$"
  raw="/etc/apt/keyrings/.docker-dearmored.$$"
  for f in gpg asc; do
    run rm -f "${tmp}"
    if run curl -fsSL --retry 2 --connect-timeout 10 \
         "${base}/linux/${ID}/${f}" -o "${tmp}"; then
      # 先当 armored 处理；dearmor 失败说明本来是二进制 keyring，直接用。
      # 用试错而不是看首字节：二进制文件走命令替换会被 bash 报 null 字节警告。
      run rm -f "${raw}"
      if ! run gpg --batch --yes --dearmor --output "${raw}" "${tmp}" 2>/dev/null; then
        run mv -f "${tmp}" "${raw}"
      fi
      run mv -f "${raw}" /etc/apt/keyrings/docker.gpg
      out=/etc/apt/keyrings/docker.gpg
      break
    fi
    run rm -f "${tmp}"
  done
  [[ -n "${out}" ]] || return 1

  # 清掉早期版本留下的 armored 文件，避免 sources.list 指向不存在的格式
  run rm -f /etc/apt/keyrings/docker.asc
  run chmod a+r "${out}"
  printf '%s\n' "${out}"
}

KEYRING=""
for mirror in "${APT_MIRRORS[@]}"; do
  [[ -n "${mirror}" ]] || continue
  if KEYRING="$(fetch_gpg "${mirror}")"; then
    APT_MIRROR="${mirror}"
    break
  fi
  warn "${mirror} 的 GPG 拿不到，换下一个源"
done
[[ -n "${KEYRING}" ]] || die "所有 apt 镜像源都拿不到 GPG：${MIRRORS_CSV}
  检查网络 / 公司代理，或显式指定：DOCKER_APT_MIRROR=https://<可达镜像> bash bin/install-docker.sh"
log "apt 源 ${APT_MIRROR}，签名密钥 ${KEYRING}"

echo "deb [arch=${ARCH} signed-by=${KEYRING}] ${APT_MIRROR}/linux/${ID} ${CODENAME} stable" \
  | run tee /etc/apt/sources.list.d/docker.list >/dev/null

# 必须先 update 才能拿到 docker-ce 的索引（docker.list 上一步才写进去）。
# 少数镜像站缺 by-hash 索引会报 "File has unexpected size / Hash Sum mismatch"，
# 所以装失败时只再 update + 装一次，不重试 install。
log "刷新 apt 索引并安装 ${APT_PKGS[*]}"
run apt-get update -qq
apt_install || {
  warn "apt 元数据可能过期（Hash Sum mismatch），再刷新一次后重试"
  run apt-get update -qq
  apt_install || die "安装 docker-ce 失败，贴出上面 apt 的报错"
}

# ---------------------------------------------------------------- daemon.json
# 这台机器直连 registry-1.docker.io 是被拒的，不配镜像加速基本拉不下镜像。
MIRRORS="${DOCKER_REGISTRY_MIRRORS:-https://docker.m.daocloud.io,https://dockerproxy.net}"
log "写入 /etc/docker/daemon.json（镜像加速 + 日志轮转 + live-restore）"
run install -d -m 0755 /etc/docker

# 代理（可选）：daemon.json 里的 proxies 只被 systemd 托管的 dockerd 读取
# （Docker >= 23.0）；手动 dockerd & 或 service 脚本启动时不生效。
# 脚本结尾会 systemctl restart docker，所以写在 proxies 里是有效的。
# DOCKER_SKIP_PROXIES=1 可以完全跳过这一段。
run python3 - "${MIRRORS}" \
  "${DOCKER_SKIP_PROXIES:-0}" \
  "${HTTP_PROXY:-${http_proxy:-}}" "${HTTPS_PROXY:-${https_proxy:-}}" <<'PY'
import json, os, shutil, sys, time

path = "/etc/docker/daemon.json"
mirrors = [m.strip() for m in sys.argv[1].split(",") if m.strip()]
skip_proxies = sys.argv[2] == "1"
http_proxy, https_proxy = sys.argv[3], sys.argv[4]

cfg = {}
if os.path.exists(path):
    shutil.copy2(path, f"{path}.bak.{time.strftime('%Y%m%d%H%M%S')}")
    try:
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        print("[!] 原 daemon.json 不是合法 JSON，已备份后重建")

cfg["log-driver"] = "json-file"
cfg["log-opts"] = {"max-size": "20m", "max-file": "3"}
cfg["live-restore"] = True
if mirrors:
    cfg["registry-mirrors"] = mirrors

# 如果当前 shell 走了代理，把代理也交给 dockerd（拉镜像走宿主代理）
if not skip_proxies and (http_proxy or https_proxy):
    p = cfg.setdefault("proxies", {})
    if http_proxy:
        p.setdefault("http-proxy", http_proxy)
    if https_proxy:
        p.setdefault("https-proxy", https_proxy)
    p.setdefault("no-proxy", "localhost,127.0.0.1,::1,*.local")

with open(path, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)
    f.write("\n")
print(open(path, encoding="utf-8").read())
PY

run systemctl enable docker >/dev/null 2>&1 || true
run systemctl restart docker || run service docker restart

# ---------------------------------------------------------------- 用户权限
TARGET_USER="${DOCKER_USER:-${SUDO_USER:-$(logname 2>/dev/null || true)}}"
TARGET_USER="${TARGET_USER:-$(id -un)}"

# 用户不存在就直接停：继续跑会静默跳过加 docker 组，最后 newgrp 时才报错
if ! id -u "${TARGET_USER}" >/dev/null 2>&1; then
  die "目标用户不存在：${TARGET_USER}
  用 DOCKER_USER=<用户名> bash bin/install-docker.sh 指定，或 DOCKER_USER=root 跳过"
fi

if [[ "${TARGET_USER}" != "root" ]]; then
  log "把 ${TARGET_USER} 加入 docker 组（下次登录生效）"
  run groupadd -f docker
  run usermod -aG docker "${TARGET_USER}"
else
  warn "目标是 root，跳过 docker 组（root 本身有权限）"
fi

# ---------------------------------------------------------------- 验证
log "版本检查"
run docker version --format '  client {{.Client.Version}} / server {{.Server.Version}}'
run docker compose version
run docker buildx version

if [[ "${DOCKER_SKIP_PULL_TEST:-0}" != "1" ]]; then
  log "拉取测试镜像（走镜像加速器，首次可能要十几秒）"
  if run timeout 180 docker run --rm hello-world >/dev/null 2>&1; then
    log "镜像拉取与容器运行测试通过"
  else
    warn "镜像拉取失败。先确认 daemon.json 里的 registry-mirrors 可用："
    warn "  cat /etc/docker/daemon.json"
    warn "  sudo docker info --format '{{json .RegistryConfig.Mirrors}}'"
    warn "  sudo docker pull docker.m.daocloud.io/library/alpine:latest"
    warn "公司有内网仓库时，用 DOCKER_REGISTRY_MIRRORS=https://your-registry bash bin/install-docker.sh 重跑"
  fi
fi

cat <<EOF

完成。接下来：

  1) 让 docker 组生效（二选一）
       newgrp docker
       或者退出重新登录

  2) 起本地湖仓基础设施（首次会构建 HMS 镜像并拉 ~3GB 镜像）
       bin/infra.sh up
       bin/infra.sh doctor

  3) 跑一遍「Spark 写 → Presto 读」（s3 profile 首次会下 ~300MB 依赖）
       cp conf/platform.properties.example conf/platform.properties
       MVN_PROFILE=s3 bin/dev-batch.sh --job all
       bin/presto-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats

  4) 详细说明与扩展方式（怎么加 Redis / ClickHouse / Kafka 等）
       infra/README.md

EOF
