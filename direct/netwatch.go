// 系统网络变化信号（三路切换信号之一）：Go 没有跨平台的系统网络事件，缺省实现用
// 「本地地址集合轮询」近似——集合变化（换 IP/换网卡/断网恢复）即产生一次通知。
// 上层可用 WithSwitchWatcher 注入平台实现（如各端自己的网络监听），缺省轮询始终可用。

package direct

import (
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// NetworkWatcher 是本机网络变化事件源：Changes 返回的通道每次收到值代表一次变化。
type NetworkWatcher interface {
	// Changes 返回变化通知通道（实现关闭时通道可关闭）。
	Changes() <-chan struct{}
}

// pollWatcher 是缺省网络变化事件源：按固定周期轮询 net.InterfaceAddrs 的地址集合。
type pollWatcher struct {
	interval time.Duration
	ch       chan struct{}
	stop     chan struct{}
	once     sync.Once
}

// newPollWatcher 起一个地址集合轮询器（interval <= 0 用缺省 1s）。
func newPollWatcher(interval time.Duration) *pollWatcher {
	if interval <= 0 {
		interval = defaultSwitchPollInterval
	}
	w := &pollWatcher{interval: interval, ch: make(chan struct{}, 1), stop: make(chan struct{})}
	go w.run()
	return w
}

// Changes 返回变化通知通道。
func (w *pollWatcher) Changes() <-chan struct{} { return w.ch }

// Close 停止轮询（幂等）。
func (w *pollWatcher) Close() error {
	w.once.Do(func() { close(w.stop) })
	return nil
}

// run 周期比较地址集合：变化时发一次非阻塞通知（不堆积）。
func (w *pollWatcher) run() {
	prev := localAddrSet()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			cur := localAddrSet()
			if cur == prev {
				continue
			}
			prev = cur
			select {
			case w.ch <- struct{}{}:
			default:
			}
		}
	}
}

// localAddrSet 返回本机全部接口地址的排序快照（用于集合比较）。
func localAddrSet() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	items := make([]string, 0, len(addrs))
	for _, a := range addrs {
		items = append(items, a.String())
	}
	sort.Strings(items)
	return strings.Join(items, "|")
}
