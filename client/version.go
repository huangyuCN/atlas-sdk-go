package client

// Version 是 SDK 客户端版本（semver 字符串，本仓唯一来源）：登录/恢复请求的
// client_version 字段（M1 协议演进，网关按 runtime.min_client_version 决定是否拒绝）
// 与需要上报客户端版本的其他场合统一取此值，不得在别处另写字面量。
// 取值与发布 tag 对齐：本仓发布 v0.7.0 tag 时同步为 0.7.0（版本线统一）。
const Version = "0.7.0"
