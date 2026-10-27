#!/usr/bin/env bash
# gen-dto：从上游生成物刷新本仓的「协议事实」，单一来源、零手写副本。
#
# 输入（只读消费，协议不在本仓定义）：
#   - 框架仓帧协议生成物：$ATLAS_DIR/transport/frame/gen/goframe/{consts,codec}_gen.go
#   - 模板仓会话协议 descriptor set：$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto
#     （会话 DTO 是**契约消息**，仍由本仓生成；两个 include 根：模板仓 + 框架仓，因它导入
#     框架仓 api/atlas/v1/route.proto 与模板仓 api/common/v1/common.proto）。
#
# 游戏域消息（battle / game / matcher）**不在本仓生成**：已迁到游戏协议包
# （模板仓 protocol/，module github.com/huangyuCN/atlas-game-layout/protocol），
# SDK 不 require、不 import 它——本脚本的产出清单就是「契约消息」的边界。
#
# 输出（全部入库；CI 有「重生成无 diff」门禁）：
#   1. frame/gen/{consts,codec}_gen.go             帧协议常量与编解码（逐字节复制生成物）
#   2. api/gateway/v1/session.pb.go                会话 DTO（模板描述符 → 本仓包）
#   3. api/gateway/v1/opclient/session.pb.go       会话 op stub + 协议描述符（op/提取器/推送 op）
#   4. api/common/v1/common.pb.go                  会话消息依赖的公共消息
#   5. api/atlas/v1/route.pb.go                    route 注解（service option 依赖）
#   6. api/lockstep/lockstep.pb.go                 帧同步消息（契约消息，会话之外的公开契约）
#   7. direct/gen/edge/{hello,flowid}.go           接入层 hello 段与 flow-id 前缀原语
#      （框架 contrib/edge 的**唯一手写实现**逐字节复制；direct 包只做薄封装，
#       避免 SDK 侧再写一份同格式字节拼装而与接入层漂移）
#
# 环境变量（默认同级相对路径；CI 用绝对路径显式指定）：
#   ATLAS_DIR            框架仓根（默认同级 ../atlas）
#   ATLAS_LAYOUT_DIR     模板仓根（默认同级 ../atlas-game-layout，会话协议唯一来源）
#   ATLAS_CLIENT_PLUGIN  protoc-gen-atlas-client 可执行文件（默认 PATH 查找，缺则从框架仓构建）
#   PROTOC_GEN_GO        protoc-gen-go 可执行文件（默认从本仓 go.mod 锁定的模块构建）
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_ROOT="$(pwd)"
SDK_MODULE="$(awk '/^module /{print $2; exit}' go.mod)"
ATLAS_DIR="${ATLAS_DIR:-$(cd .. && pwd)/atlas}"
ATLAS_LAYOUT_DIR="${ATLAS_LAYOUT_DIR:-$(cd .. && pwd)/atlas-game-layout}"

for d in "$ATLAS_DIR" "$ATLAS_LAYOUT_DIR"; do
  if [ ! -d "$d" ]; then
    echo "gen-dto: 上游仓不存在：${d}（用 ATLAS_DIR / ATLAS_LAYOUT_DIR 指定；CI 下由 workflow 的 job env 给出绝对路径）" >&2
    exit 1
  fi
done
command -v protoc >/dev/null 2>&1 || { echo "gen-dto: 需要 protoc（protobuf-compiler）" >&2; exit 1; }

TMPDIR_GEN="$(mktemp -d -t atlas-gendto.XXXXXX)"
trap 'rm -rf "$TMPDIR_GEN"' EXIT

# 1) 帧协议常量与编解码：框架生成物逐字节复制到仓内固定路径（frame 包只做引用）。
mkdir -p frame/gen
rm -f frame/gen/frame_gen.go
cp "$ATLAS_DIR/transport/frame/gen/goframe/consts_gen.go" frame/gen/consts_gen.go
cp "$ATLAS_DIR/transport/frame/gen/goframe/codec_gen.go" frame/gen/codec_gen.go

# 1b) 接入层 hello 段与 flow-id 前缀：框架 contrib/edge 的手写实现逐字节复制
#     （hello.go/flowid.go 自成一包、仅标准库依赖）。SDK 的 direct 包只做薄封装，
#     线格式唯一来源仍是框架；本步与帧生成物同款受 CI「重生成无 diff」门禁约束。
mkdir -p direct/gen/edge
cp "$ATLAS_DIR/contrib/edge/hello.go" direct/gen/edge/hello.go
cp "$ATLAS_DIR/contrib/edge/flowid.go" direct/gen/edge/flowid.go
# 被复制文件不带包注释（框架的包注释在未复制的 edge.go 里），这里补一份同步生成的 doc.go，
# 满足本仓 comment-lint（缺包注释即失败）。
cat > direct/gen/edge/doc.go <<'EOF'
// Package edge 是本仓对框架 contrib/edge 客户端侧原语（hello 段与 flow-id 前缀）的**逐字节同步副本**，
// 由 scripts/gen-dto.sh 从框架仓复制生成（含本文件），不得手工编辑：线格式的唯一手写实现仍在
// 框架 contrib/edge，本包只负责让 SDK 复用同一份编解码，避免第二份实现漂移。
package edge
EOF
echo "帧协议生成物 → frame/gen/{consts,codec}_gen.go"

# 2) 插件：protoc-gen-go 一律从本仓 go.mod 锁定的 protobuf 模块构建——生成物头部会
#    记录插件版本，PATH 上的版本（如 v1.36.10）会让「重生成无 diff」门禁误报；
#    protoc-gen-atlas-client 优先取显式指定/PATH，否则从框架仓当前检出构建。
PROTOC_GEN_GO="${PROTOC_GEN_GO:-}"
if [ -z "$PROTOC_GEN_GO" ]; then
  PROTOC_GEN_GO="$TMPDIR_GEN/protoc-gen-go"
  go build -o "$PROTOC_GEN_GO" google.golang.org/protobuf/cmd/protoc-gen-go
fi
PLUGIN="${ATLAS_CLIENT_PLUGIN:-}"
if [ -z "$PLUGIN" ]; then
  if command -v protoc-gen-atlas-client >/dev/null 2>&1; then
    PLUGIN="$(command -v protoc-gen-atlas-client)"
  else
    PLUGIN="$TMPDIR_GEN/protoc-gen-atlas-client"
    (cd "$ATLAS_DIR" && go build -o "$PLUGIN" ./cmd/protoc-gen-atlas-client)
  fi
fi

# 3) 协议 descriptor set：模板仓导出（两个 include 根，见文件头）；会话 DTO 与
#    lockstep 契约消息同集导出（lockstep 不再被会话消息 import，必须显式列出才会进集合）。
DESC="$TMPDIR_GEN/gateway.desc"
protoc --descriptor_set_out="$DESC" --include_imports \
  -I "$ATLAS_LAYOUT_DIR" -I "$ATLAS_DIR" \
  "$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto" \
  "$ATLAS_DIR/api/lockstep/lockstep.proto"
echo "descriptor set → 模板仓 + 框架仓导出（session.proto + lockstep.proto）"

# 4) DTO：模板/框架 go_package 经 M 映射到本仓本地包（不依赖模板 module 与框架 module），
#    module= 去掉模块前缀后落到 api/**（路径与 proto 目录一致）。
rm -rf api/gateway/v1 api/common/v1 api/atlas/v1 api/lockstep
mkdir -p api
protoc --descriptor_set_in="$DESC" \
  --plugin=protoc-gen-go="$PROTOC_GEN_GO" \
  --go_out=. --go_opt=module="$SDK_MODULE" \
  --go_opt=Mapi/gateway/v1/session.proto="$SDK_MODULE/api/gateway/v1;gatewayv1" \
  --go_opt=Mapi/common/v1/common.proto="$SDK_MODULE/api/common/v1;commonv1" \
  --go_opt=Mapi/atlas/v1/route.proto="$SDK_MODULE/api/atlas/v1;atlasroutepb" \
  --go_opt=Mapi/lockstep/lockstep.proto="$SDK_MODULE/api/lockstep;locksteppb" \
  api/gateway/v1/session.proto \
  api/common/v1/common.proto api/atlas/v1/route.proto api/lockstep/lockstep.proto
echo "DTO → api/gateway/v1、api/common/v1、api/atlas/v1、api/lockstep"

# 5) op stub + 协议描述符：go_client_package 指向本仓 client 包；
#    paths=source_relative 让产物落在 proto 目录的 opclient/ 子包。
protoc --descriptor_set_in="$DESC" \
  --plugin=protoc-gen-atlas-client="$PLUGIN" \
  --atlas-client_out=. \
  --atlas-client_opt=paths=source_relative \
  --atlas-client_opt=Mapi/gateway/v1/session.proto="$SDK_MODULE/api/gateway/v1;gatewayv1" \
  --atlas-client_opt=Mapi/common/v1/common.proto="$SDK_MODULE/api/common/v1;commonv1" \
  --atlas-client_opt=Mapi/atlas/v1/route.proto="$SDK_MODULE/api/atlas/v1;atlasroutepb" \
  --atlas-client_opt=Mapi/lockstep/lockstep.proto="$SDK_MODULE/api/lockstep;locksteppb" \
  --atlas-client_opt=go_client_package="$SDK_MODULE/client" \
  api/gateway/v1/session.proto
echo "op stub + 协议描述符 → api/gateway/v1/opclient"

# 改动计数排除 CI 检出的上游目录（它们不是本仓产物，见 workflow 门禁同款 pathspec）。
echo "生成完成（重跑本脚本应无 diff；当前工作区改动：$(git -C "$REPO_ROOT" status --porcelain -- . ':!atlas' ':!atlas-game-layout' | wc -l | tr -d ' ') 处）"
