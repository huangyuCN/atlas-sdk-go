package client

import (
	"time"
)

// heartbeatLoop 周期发送传输保活心跳（Ping），连续失败视为死链并关闭连接。
// 注意：
//   - 传输心跳不续租业务会话（会话续租走登录协议的业务 Heartbeat，
//     由业务层调度，见规范 §5.2 双层心跳）；
//   - 死链路径调用 shutdown 而非 Close（评审 B1 修复）：
//     本 goroutine 在 wg 内，若调用 Close 的 wg.Wait 会自等待死锁。
func (c *Client) heartbeatLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	var failures int
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			if err := c.Invoke(nil, HeartbeatOperation, nil, nil); err != nil {
				failures++
				if failures >= defaultHeartbeatFailures {
					// 死链：只做 shutdown（关连接、停循环、回收 in-flight），
					// 不等待 wg（自身在 wg 内），由外部 Close 的 wg.Wait 收口。
					_ = c.shutdown()
					return
				}
				continue
			}
			failures = 0
		}
	}
}
