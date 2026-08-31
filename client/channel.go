package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// channel 是连接本体：一条通道的连接代际、读写循环、心跳、请求匹配与重连监管。
// Client 是编排器（门面）；本类型不对外暴露，外部经 ChannelView 操作。
// 每条通道独立心跳与重连（规范 §5.2 dual 双通道独立重连）。
type channel struct {
	kind Kind
	addr string

	// dialFn 建立传输连接（首连与重连共用；按通道配置路由 TCP/WS）。
	dialFn func(ctx context.Context) (channelTransport, error)

	// 当前代快照：Invoke/读循环/心跳统一经 gen() 获取（评审 B3 修复）。
	genPtr atomic.Pointer[generation]

	writeMu sync.Mutex // 帧级写锁：并发 Invoke 下整帧不交错

	serial Serializer
	// inflight 表按 (epoch, seq) 匹配（规范 §5.2）：值使用单一所有权语义——
	// 认领（LoadAndDelete）后发送，timer/ctx/断连/迟到响应四方竞争恰好一次投递。
	inflight sync.Map // inflightKey → chan invokeResult
	seq      atomic.Uint32

	notifyMu      sync.Mutex
	notifies      map[string]map[uintptr]notifyEntry // op → handler 集合（函数值指针幂等去重）
	onReadExitPtr atomic.Pointer[func(error)]

	// 重连配置与状态。
	autoReconnect     bool
	backoffBase       time.Duration
	backoffMax        time.Duration
	queueSize         int
	queue             chan *queuedInvoke
	onReconnected     func() error   // 重连成功后的会话钩子（业务重登 / Join 重绑定）
	hookTimeout       time.Duration  // 钩子执行超时上限
	state             atomic.Int32   // State
	hookActive        atomic.Bool    // 会话钩子正在同步执行（钩子内 Invoke 放行直通，评审 v0.3-B1 配套）
	closed            atomic.Bool    // 关闭标记（幂等）
	closeCh           chan struct{}  // 关闭信号（本通道所有 goroutine 的退出源）
	lifecycleMu       sync.Mutex     // 序列化 wg.Add 与 Close 的 wg.Wait（WaitGroup 并发纪律）
	wg                sync.WaitGroup // supervisor + 每代读循环/心跳 + 钩子执行
	genMu             sync.Mutex     // setConn 的代际切换互斥（切换与 wg.Add 原子）
	heartbeatInterval time.Duration
	invokeTimeout     time.Duration
	maxBodySize       int

	sessionHBInterval time.Duration
	sessionHBOp       func() (string, any)
}

// generation 是一次连接代际的完整快照（评审 B3 修复）：
// epoch / 连接 / 死亡信号绑定在同一个不可变结构里，Invoke 通过单次
// atomic Load 取齐三项，杜绝「旧 epoch + 新连接」的撕裂读。
// 代际切换 = 整体替换该指针（setConn），旧的 generation 永不修改。
type generation struct {
	epoch uint64
	tr    channelTransport
	done  chan struct{} // 读循环退出时关闭
}

// inflightKey 是 in-flight 表的复合键：(epoch, seq)。
type inflightKey struct {
	epoch uint64
	seq   uint32
}

// notifyEntry 是订阅表条目：以函数值本身判等（配合 reflect 型指针比较实现幂等）。
type notifyEntry struct {
	fn NotifyHandler
}

// newChannel 构建单条通道：合并默认与通道级配置、建立首连、启动读循环/心跳/监管。
// 首连失败返回错误（调用方决定整体回滚）。
func newChannel(cfg ChannelConfig, defaults []Option) (*channel, error) {
	s := defaultSettings()
	s.transport = cfg.Transport
	s.addr = cfg.Addr
	s.path = cfg.Path
	s.apply(defaults)
	s.apply(cfg.Opts)
	if s.addr == "" {
		return nil, fmt.Errorf("client: 通道 %s 缺少服务端地址", cfg.Kind)
	}
	tr, err := dialTransport(context.Background(), s.transport, s.addr, s.path, s.maxBodySize)
	if err != nil {
		return nil, fmt.Errorf("client: 通道 %s 拨号失败: %w", cfg.Kind, err)
	}
	ch := &channel{
		kind:              cfg.Kind,
		addr:              s.addr,
		serial:            s.serial,
		heartbeatInterval: s.heartbeatInterval,
		invokeTimeout:     s.invokeTimeout,
		maxBodySize:       s.maxBodySize,
		autoReconnect:     s.autoReconnect,
		backoffBase:       s.backoffBase,
		backoffMax:        s.backoffMax,
		queueSize:         s.queueSize,
		hookTimeout:       s.hookTimeout,
		onReconnected:     s.onReconnected,
		sessionHBInterval: s.sessionHeartbeatInterval,
		sessionHBOp:       s.sessionHeartbeatOp,
		notifies:          make(map[string]map[uintptr]notifyEntry),
		closeCh:           make(chan struct{}),
	}
	ch.dialFn = func(ctx context.Context) (channelTransport, error) {
		return dialTransport(ctx, s.transport, s.addr, s.path, s.maxBodySize)
	}
	ch.queue = make(chan *queuedInvoke, ch.queueSize)

	// 首代连接：generation 在本 goroutine 构造（代际切换只在 supervisor/Dial 串行发生）。
	ch.setConn(tr)
	ch.state.Store(int32(StateConnected)) // 首连已在手，置位先于 supervisor 调度
	ch.wg.Add(1)
	go ch.supervisor()
	return ch, nil
}

// setConn 登记新一代连接：构造不可变 generation 整体替换 genPtr。
// 必须在 Dial/supervisor 主线程调用（代际切换串行化；epoch 单调递增）。
func (ch *channel) setConn(tr channelTransport) {
	ch.genMu.Lock()
	defer ch.genMu.Unlock()
	done := make(chan struct{})
	prev := ch.genPtr.Load()
	epoch := uint64(1)
	if prev != nil {
		epoch = prev.epoch + 1
	}
	ch.genPtr.Store(&generation{epoch: epoch, tr: tr, done: done})
}

// gen 返回当前代快照（单次原子读，评审 B3 修复：杜绝撕裂读）。
func (ch *channel) gen() *generation { return ch.genPtr.Load() }

// closeConn 关闭当前代连接（触发读循环退出 → supervisor 重连）。
func (ch *channel) closeConn() {
	if g := ch.gen(); g != nil {
		_ = g.tr.Close()
	}
}

// supervisor 是连接生命周期监管：监视连接死亡并驱动自动重连。
func (ch *channel) supervisor() {
	defer ch.wg.Done()
	g := ch.gen()
	ch.startConnLoops(g)

	for {
		select {
		case <-ch.closeCh:
			return
		case <-g.done:
			if ch.closed.Load() {
				return
			}
			// 连接死亡（对端断开/读错误/心跳死链）：进入重连。
			// （协议级致命错误由 readLoop 直接 terminate 本通道，不经此路径。）
			if !ch.autoReconnect {
				ch.state.Store(int32(StateDisconnected))
				return
			}
			ch.state.Store(int32(StateReconnecting))
			ng, ok := ch.reconnectOnce()
			if !ok {
				ch.state.Store(int32(StateDisconnected))
				return
			}
			// 评审 B6 修复：新连接读循环可能已在钩子/置位前遇到协议错误并 terminate
			// （closed=true, state=Disconnected）——不得覆盖回 Connected。
			if ch.closed.Load() {
				return
			}
			// 评审 v0.3-B1 修复：钩子成功前不 drain（排队请求等待「重连+重登成功」后重发）。
			// 每轮循环：置 Connected（连接可用，钩子内 Invoke 可直写）→ 同步钩子 →
			// 失败则置 Reconnecting、弃用本代、退避重连后重试（排队请求保留）。
			// 钩子失败无限重试（规范「返回错误视为本次重连未完成，SDK 继续退避重试」），
			// 退避间隔天然限流；Close 可随时打断。
			for {
				ch.genMu.Lock()
				if ch.closed.Load() {
					// 双重检查：genMu 等待期间可能发生 terminate（评审 B6）。
					ch.genMu.Unlock()
					return
				}
				ch.state.Store(int32(StateConnected))
				ch.genMu.Unlock()
				if ch.onReconnected == nil {
					break
				}
				if err := ch.runHookSync(ng); err == nil {
					break // 钩子成功
				}
				// 钩子失败：排队请求保留（未重发、无副作用）；弃用本代、退避、重连后重试。
				ch.state.Store(int32(StateReconnecting))
				ch.closeGeneration(ng.done)
				ng2, ok2 := ch.reconnectFrom(ch.backoffBase)
				if !ok2 {
					ch.state.Store(int32(StateDisconnected))
					return
				}
				ng = ng2
				if ch.closed.Load() {
					return
				}
			}
			// 钩子成功（或无钩子）：此时才 drain——排队请求按「重连+重登成功」语义重发。
			ch.drainQueue()
			g = ng // 进入下一代监视
		}
	}
}

// reconnectFrom 从指定退避起点继续重连（钩子失败后的继续退避入口）。
func (ch *channel) reconnectFrom(backoff time.Duration) (*generation, bool) {
	for {
		if err := sleepInterruptible(jitter(backoff), ch.closeCh); err != nil {
			return nil, false
		}
		tr, err := ch.dialFn(ch.rebindDialCtx())
		if err != nil {
			backoff = ch.nextBackoff(backoff)
			continue
		}
		ch.lifecycleMu.Lock()
		if ch.closed.Load() {
			ch.lifecycleMu.Unlock()
			_ = tr.Close()
			return nil, false
		}
		ch.setConn(tr)
		ng := ch.gen()
		ch.startConnLoops(ng)
		ch.lifecycleMu.Unlock()
		return ng, true
	}
}

// startConnLoops 为当前代连接启动读循环、传输心跳与会话心跳（每代）。
func (ch *channel) startConnLoops(g *generation) {
	n := 2
	if ch.sessionHBInterval > 0 && ch.sessionHBOp != nil {
		n = 3
	}
	ch.wg.Add(n)
	go ch.readLoop(g)
	if ch.heartbeatInterval > 0 {
		go ch.heartbeatLoop(g)
	} else {
		ch.wg.Done()
	}
	if ch.sessionHBInterval > 0 && ch.sessionHBOp != nil {
		go ch.sessionHeartbeatLoop(g)
	}
}

// sessionHeartbeatLoop 会话心跳调度（规范 §5.2 双层心跳的业务层）：
// 周期 Invoke 业务 Heartbeat（op/req 由 opFactory 提供，业务侧闭包携带最新 token）；
// 业务错误（*BusinessError，如会话过期）触发重登钩子（会话失效语义）；
// 网络错误静默（连接死亡由重连机制接管）。仅业务通道配置时启动。
func (ch *channel) sessionHeartbeatLoop(g *generation) {
	defer ch.wg.Done()
	ticker := time.NewTicker(ch.sessionHBInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.done: // 本代连接死亡：随代退出（新代由 startConnLoops 重新启动）
			return
		case <-ch.closeCh:
			return
		case <-ticker.C:
			op, req := ch.sessionHBOp()
			if op == "" {
				continue // 工厂未就绪（如尚未登录无 token）：跳过本轮
			}
			err := ch.invokeOnce(context.Background(), op, req, nil)
			if be, ok := err.(*BusinessError); ok {
				// 会话失效：触发重登钩子（与重连钩子同一入口，业务侧刷新 token）。
				// 异步执行避免阻塞心跳 ticker；失败静默（下一轮心跳再触发）。
				go func(be *BusinessError) {
					defer func() { _ = recover() }()
					_ = ch.onReconnected()
				}(be)
			}
			// NetworkError/TimeoutError 静默：重连机制处理
		}
	}
}

// reconnectOnce 执行一轮「退避重连 + 新连接登记」；返回新一代与是否继续。
// 会话钩子不在本函数执行（评审 B1/B2：钩子异步化，见 supervisor）。
func (ch *channel) reconnectOnce() (*generation, bool) {
	backoff := ch.backoffBase
	for {
		// 退避等待（带抖动抖开重连风暴）；期间关闭则立即退出。
		if err := sleepInterruptible(jitter(backoff), ch.closeCh); err != nil {
			return nil, false
		}
		// 可取消拨号（评审 v0.3-B4 修复）：Close 不被系统拨号阻塞。
		tr, err := ch.dialFn(ch.rebindDialCtx())
		if err != nil {
			backoff = ch.nextBackoff(backoff)
			continue
		}
		ch.lifecycleMu.Lock()
		if ch.closed.Load() {
			ch.lifecycleMu.Unlock()
			_ = tr.Close()
			return nil, false
		}
		// startConnLoops（含 wg.Add）必须在锁内完成：
		// 保证 Close 的 wg.Wait 开始后不可能再有 Add（WaitGroup 并发纪律）。
		ch.setConn(tr)
		ng := ch.gen()
		ch.startConnLoops(ng)
		ch.lifecycleMu.Unlock()
		return ng, true
	}
}

// nextBackoff 计算下一轮退避时长（×2 封顶；抖动只在睡眠时应用一次，
// 评审 Important 修复：不再在保存下一档时重复抖动）。
func (ch *channel) nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > ch.backoffMax {
		next = ch.backoffMax
	}
	return next
}

// State 返回本通道连接状态。
func (ch *channel) State() State { return State(ch.state.Load()) }

// nextSeq 分配下一个请求 seq；回绕到 0 时跳过（0 为协议非法值）。
// 2^32-1 次请求后才可能触发，防御性处理。
func (ch *channel) nextSeq() uint32 {
	for {
		s := ch.seq.Add(1)
		if s != 0 {
			return s
		}
	}
}

// Close 优雅关闭本通道：停止重连与心跳、断开当前连接、取消全部 in-flight 与排队请求。
// 幂等；任何时刻调用都不会死锁（钩子异步化后 supervisor 不再被业务回调阻塞；
// wg.Wait 的每一项都能在 closeCh 关闭后独立退出）。
func (ch *channel) Close() error {
	ch.lifecycleMu.Lock()
	if ch.closed.Swap(true) {
		ch.lifecycleMu.Unlock()
		ch.wg.Wait()
		return nil
	}
	close(ch.closeCh)
	if g := ch.gen(); g != nil {
		_ = g.tr.Close()
	}
	ch.lifecycleMu.Unlock()

	ch.wg.Wait() // supervisor/读循环/心跳/钩子执行器均监听 closeCh 或连接关闭
	ch.failAllInflight(errors.New("client: 连接已关闭"))
	ch.failAllQueued()
	ch.state.Store(int32(StateDisconnected))
	return nil
}

// trOfDone 按代信号通道精确查找对应代的传输（钩子失败只弃用自己那一代）。
// 找不到（该代已被换代替换）返回 nil，调用方跳过关闭。
func (ch *channel) trOfDone(done chan struct{}) channelTransport {
	g := ch.genPtr.Load()
	if g != nil && g.done == done {
		return g.tr
	}
	return nil
}

// closeGeneration 弃用与 done 匹配的那一代连接（nil 安全：代已被替换则无事可做）。
func (ch *channel) closeGeneration(done chan struct{}) {
	if tr := ch.trOfDone(done); tr != nil {
		_ = tr.Close()
	}
}

// runHookSync 同步执行会话钩子（重登/Join）：带超时 + panic 保护。
// 同步语义是评审 v0.3-B1 修复的核心：钩子成功返回后才置 Connected 并 drain，
// 保证排队请求在「重连+重登成功」后才重发。钩子在 supervisor goroutine 内执行，
// 超时由 hookTimeout 强制收敛（超时视为失败，弃用本代连接）。
func (ch *channel) runHookSync(g *generation) (err error) {
	defer func() {
		ch.hookActive.Store(false)
		if r := recover(); r != nil {
			err = fmt.Errorf("client: 重连钩子 panic: %v", r)
		}
	}()
	ch.hookActive.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- ch.onReconnected()
	}()
	timer := time.NewTimer(ch.hookTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("client: 重连钩子超时（%s）", ch.hookTimeout)
	case <-ch.closeCh:
		return errors.New("client: 连接已关闭")
	}
}

// rebindDialCtx 返回可取消的重连拨号上下文（评审 v0.3-B4 修复）：
// 绑定通道关闭信号，Close 后进行中的拨号被取消，不被系统 TCP/WS 握手阻塞。
func (ch *channel) rebindDialCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ch.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}
