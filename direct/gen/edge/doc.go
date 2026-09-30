// Package edge 是本仓对框架 contrib/edge 客户端侧原语（hello 段与 flow-id 前缀）的**逐字节同步副本**，
// 由 scripts/gen-dto.sh 从框架仓复制生成（含本文件），不得手工编辑：线格式的唯一手写实现仍在
// 框架 contrib/edge，本包只负责让 SDK 复用同一份编解码，避免第二份实现漂移。
package edge
