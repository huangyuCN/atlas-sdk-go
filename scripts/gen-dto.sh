#!/usr/bin/env bash
# gen-dto：从上游生成物刷新本仓的「协议事实」，单一来源、零手写副本。
#
# 输入（只读消费，协议不在本仓定义）：
#   - 框架仓帧协议生成物：$ATLAS_DIR/transport/frame/gen/goframe/{consts,codec}_gen.go
#   - 模板仓会话/战斗协议 descriptor set：$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto
#     与 $ATLAS_LAYOUT_DIR/api/battle/v1/battle_service.proto（examples 冒烟的战斗
#     绑定探针需要权威 DTO）。两者都导入框架仓的 api/atlas/v1/route.proto、后者还导入
#     框架仓 api/lockstep/lockstep.proto，故导出 descriptor set 必须同时给模板仓与
#     框架仓两个 include 根。
#
# 输出（全部入库；CI 有「重生成无 diff」门禁）：
#   1. frame/gen/{consts,codec}_gen.go             帧协议常量与编解码（逐字节复制生成物）
#   2. api/gateway/v1/session.pb.go                会话 DTO（模板描述符 → 本仓包）
#   3. api/gateway/v1/opclient/session.pb.go       会话 op stub + 协议描述符（op/提取器/推送 op）
#   4. api/battle/v1/{battle_service,battle}.pb.go 战斗 DTO（冒烟战斗探针）
#   5. api/battle/v1/opclient/battle_service.pb.go 战斗 op stub
#   6. api/common/v1/common.pb.go                  LoginReply.player 依赖的公共消息
#   7. api/atlas/v1/route.pb.go                    route 注解（service option 依赖）
#   8. api/lockstep/lockstep.pb.go                 帧同步消息（battle_service 的 import 依赖）
#
# 环境变量（默认同级相对路径；CI 用绝对路径显式指定）：
#   ATLAS_DIR            框架仓根（默认同级 ../atlas）
#   ATLAS_LAYOUT_DIR     模板仓根（默认同级 ../atlas-game-layout，会话/战斗协议唯一来源）
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

# 3) 协议 descriptor set：模板仓导出（两个 include 根，见文件头）；会话与战斗同集导出。
DESC="$TMPDIR_GEN/gateway.desc"
protoc --descriptor_set_out="$DESC" --include_imports \
  -I "$ATLAS_LAYOUT_DIR" -I "$ATLAS_DIR" \
  "$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto" \
  "$ATLAS_LAYOUT_DIR/api/battle/v1/battle_service.proto"
echo "descriptor set → 模板仓导出（session.proto + battle_service.proto）"

# 4) DTO：模板/框架 go_package 经 M 映射到本仓本地包（不依赖模板 module 与框架 module），
#    module= 去掉模块前缀后落到 api/**（路径与 proto 目录一致）。
rm -rf api/gateway/v1 api/battle/v1 api/common/v1 api/atlas/v1 api/lockstep
mkdir -p api
protoc --descriptor_set_in="$DESC" \
  --plugin=protoc-gen-go="$PROTOC_GEN_GO" \
  --go_out=. --go_opt=module="$SDK_MODULE" \
  --go_opt=Mapi/gateway/v1/session.proto="$SDK_MODULE/api/gateway/v1;gatewayv1" \
  --go_opt=Mapi/battle/v1/battle_service.proto="$SDK_MODULE/api/battle/v1;battlev1" \
  --go_opt=Mapi/battle/v1/battle.proto="$SDK_MODULE/api/battle/v1;battlev1" \
  --go_opt=Mapi/common/v1/common.proto="$SDK_MODULE/api/common/v1;commonv1" \
  --go_opt=Mapi/atlas/v1/route.proto="$SDK_MODULE/api/atlas/v1;atlasroutepb" \
  --go_opt=Mapi/lockstep/lockstep.proto="$SDK_MODULE/api/lockstep;locksteppb" \
  api/gateway/v1/session.proto api/battle/v1/battle_service.proto api/battle/v1/battle.proto \
  api/common/v1/common.proto api/atlas/v1/route.proto api/lockstep/lockstep.proto
echo "DTO → api/gateway/v1、api/battle/v1、api/common/v1、api/atlas/v1、api/lockstep"

# 5) op stub + 协议描述符：go_client_package 指向本仓 client 包；
#    paths=source_relative 让产物落在 proto 目录的 opclient/ 子包。
protoc --descriptor_set_in="$DESC" \
  --plugin=protoc-gen-atlas-client="$PLUGIN" \
  --atlas-client_out=. \
  --atlas-client_opt=paths=source_relative \
  --atlas-client_opt=Mapi/gateway/v1/session.proto="$SDK_MODULE/api/gateway/v1;gatewayv1" \
  --atlas-client_opt=Mapi/battle/v1/battle_service.proto="$SDK_MODULE/api/battle/v1;battlev1" \
  --atlas-client_opt=Mapi/battle/v1/battle.proto="$SDK_MODULE/api/battle/v1;battlev1" \
  --atlas-client_opt=Mapi/common/v1/common.proto="$SDK_MODULE/api/common/v1;commonv1" \
  --atlas-client_opt=Mapi/atlas/v1/route.proto="$SDK_MODULE/api/atlas/v1;atlasroutepb" \
  --atlas-client_opt=Mapi/lockstep/lockstep.proto="$SDK_MODULE/api/lockstep;locksteppb" \
  --atlas-client_opt=go_client_package="$SDK_MODULE/client" \
  api/gateway/v1/session.proto api/battle/v1/battle_service.proto
echo "op stub + 协议描述符 → api/gateway/v1/opclient、api/battle/v1/opclient"

# 改动计数排除 CI 检出的上游目录（它们不是本仓产物，见 workflow 门禁同款 pathspec）。
echo "生成完成（重跑本脚本应无 diff；当前工作区改动：$(git -C "$REPO_ROOT" status --porcelain -- . ':!atlas' ':!atlas-game-layout' | wc -l | tr -d ' ') 处）"
