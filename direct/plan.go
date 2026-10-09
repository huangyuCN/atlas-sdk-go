// Package direct 实现战斗帧直连会话（阶段 3）：客户端从成局推送取「本局票据 + 接入层各面地址」，
// 按传输面直连接入层（WS 走升级 query、KCP/UDP 走 hello 段 + flow-id），逐帧在帧会话槽携带票据
// 跑战斗 op（JoinBattle / SendFrameInput / SyncFrames），断线后重新 hello 并重放入局与补帧。
//
// 地址的唯一来源是**本局推送**（不读本地配置）；缺面即明确报错、不猜端口、不静默换面。
package direct

import (
	"errors"
	"fmt"
	"strings"
)

// Transport 是接入层传输面，取值与 battle.v1.EdgeTransport 枚举一一对应
// （protojson 下发的是自解释的枚举名字符串）。
type Transport uint8

// 全部传输面取值（与 EDGE_TRANSPORT_* 同号）。
const (
	// TransportWS 是 WebSocket 面（TCP 承载，票据走升级请求 query / 头）。
	TransportWS Transport = 1
	// TransportKCP 是 KCP 面（可靠 UDP，hello 段后在同一 socket 上跑 KCP 会话）。
	TransportKCP Transport = 2
	// TransportUDP 是裸 UDP 面（首包 hello 换 flow-id，之后双向带前缀）。
	TransportUDP Transport = 3
)

// 直连层的错误哨兵（errors.Is 判定）：解析类、握手段与业务语义类分开。
var (
	// ErrTransportUnknown 表示传输面名字无法识别（既不是枚举名也不是短名）。
	ErrTransportUnknown = errors.New("direct: 未知传输面")
	// ErrTransportNotFound 表示本局未下发该传输面的接入层地址（不得猜端口、不得静默换面）。
	ErrTransportNotFound = errors.New("direct: 本局未下发该传输面的接入层地址")
	// ErrNotifyMalformed 表示成局推送载荷不是可解析的 JSON 对象。
	ErrNotifyMalformed = errors.New("direct: 成局推送载荷非法")
	// ErrNotifyNoTicket 表示成局推送缺少 battle_ticket 或票为空（空票连不上接入层）。
	ErrNotifyNoTicket = errors.New("direct: 成局推送缺少 battle_ticket")
	// ErrNotifyNoEndpoint 表示成局推送没有可用的接入层面地址（面未知或地址为空）。
	ErrNotifyNoEndpoint = errors.New("direct: 成局推送缺少可用的接入层面地址")
	// ErrFlowIDMismatch 表示数据报前缀与本流 flow-id 不符（失配即丢弃并要求重新 hello）。
	ErrFlowIDMismatch = errors.New("direct: 数据报 flow-id 前缀失配")
	// ErrRejected 表示被接入层拒绝（无应用层回执、连接被断）：票无效/过期/后端不可用，
	// 或接入层不可达时同形。**不可重试**，上层应回业务链路重新匹配取新票。
	ErrRejected = errors.New("direct: 被接入层拒绝（无回执，连接被断）")
	// ErrTicketExpired 表示 battle 侧判定票据过期（reason BATTLE_TICKET_EXPIRED）：
	// 需回业务链路重新匹配取新票，重连无意义。
	ErrTicketExpired = errors.New("direct: 战斗票据已过期")
	// ErrTicketInvalid 表示 battle 侧判定票据非法（reason BATTLE_TICKET_INVALID）。
	ErrTicketInvalid = errors.New("direct: 战斗票据非法")
	// ErrClosed 表示会话已关闭。
	ErrClosed = errors.New("direct: 会话已关闭")
	// 终态族哨兵（ErrBattleEnded / ErrBattleNotFound / ErrBattleFull /
	// ErrFrameTargetMismatch）见 terminal.go：它们共享「可判定 + 终态化」口径，
	// 故与解析/握手/票据类哨兵分文件声明。
)

// 协议常量：与服务端 battle 侧同源（枚举名，非魔法值散落）。
const (
	// reasonTicketExpired 是 battle 侧票据过期 reason（api/error/v1 的枚举名）。
	reasonTicketExpired = "BATTLE_TICKET_EXPIRED"
	// reasonTicketInvalid 是 battle 侧票据非法 reason（api/error/v1 的枚举名）。
	reasonTicketInvalid = "BATTLE_TICKET_INVALID"
)

// String 返回传输面短名（ws/kcp/udp；日志与选项口径）。
func (t Transport) String() string {
	switch t {
	case TransportWS:
		return "ws"
	case TransportKCP:
		return "kcp"
	case TransportUDP:
		return "udp"
	default:
		return fmt.Sprintf("transport(%d)", uint8(t))
	}
}

// ParseTransport 把枚举名字符串（EDGE_TRANSPORT_WS 等）或短名（ws/kcp/udp）解析为传输面；
// 未指定与未知取值都返回 ErrTransportUnknown（不静默回落）。
func ParseTransport(name string) (Transport, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "EDGE_TRANSPORT_WS", "WS":
		return TransportWS, nil
	case "EDGE_TRANSPORT_KCP", "KCP":
		return TransportKCP, nil
	case "EDGE_TRANSPORT_UDP", "UDP":
		return TransportUDP, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrTransportUnknown, name)
	}
}

// Plan 是一次直连作战所需的全部输入，全部来自本局成局推送（不读本地配置）。
type Plan struct {
	// MatchID 是对局标识（观测/排障用）。
	MatchID string
	// BattleID 是战斗标识（JoinBattle/SyncFrames 的寻址键）。
	BattleID string
	// Ticket 是本玩家本人的入场票密文（AEAD 密文，接入层与 battle 共持密钥验票）。
	Ticket []byte
	// Endpoints 是「传输面 → 接入层地址（host:port）」列表（本局推送下发，唯一来源）。
	Endpoints map[Transport]string
}

// Endpoint 返回指定传输面的接入层地址；本局未下发该面即报错（不猜端口、不静默换面）。
func (p Plan) Endpoint(t Transport) (string, error) {
	addr, ok := p.Endpoints[t]
	if !ok || addr == "" {
		return "", fmt.Errorf("%w: %s", ErrTransportNotFound, t)
	}
	return addr, nil
}

// defaultPriority 是未显式指定面时的取面顺序（第一个在计划里存在的面）。
var defaultPriority = []Transport{TransportWS, TransportKCP, TransportUDP}

// pickEndpoint 按显式面或默认优先级选出本次直连的「面 + 地址」。
func pickEndpoint(p Plan, want Transport) (Transport, string, error) {
	if want != 0 {
		addr, err := p.Endpoint(want)
		return want, addr, err
	}
	for _, t := range defaultPriority {
		if addr, err := p.Endpoint(t); err == nil {
			return t, addr, nil
		}
	}
	return 0, "", fmt.Errorf("%w: 本局未下发任何支持的面", ErrTransportNotFound)
}
