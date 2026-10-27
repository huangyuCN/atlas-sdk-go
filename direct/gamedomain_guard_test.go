// 依赖边界守卫：游戏域消息（battle / game / matcher）已迁出 SDK，改由游戏协议包
// （github.com/huangyuCN/atlas-game-layout/protocol）按 .proto 生成；SDK 只留传输/会话 +
// 契约消息（frame / edge / session / common / route / lockstep）。
//
// 本用例把这次迁移的**结果**钉死（而不是钉实现）：仓内既不得再有游戏域生成包目录，
// 也不得再 import 它们——手滑把生成物加回来、或新增代码又把游戏域类型引回来，这里即红。
// 判定口径是「文件路径 + import 路径」，与协议包在本地用什么 module 路径无关。

package direct

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// sdkModule 与 protocolModule 分开两处声明：本用例以「引号 + import 路径」为判定口径，
// 拼接出来的字符串不会误伤本文件自己。
const (
	sdkModule      = "github.com/huangyuCN/atlas-sdk-go"
	protocolModule = "github.com/huangyuCN/atlas-game-layout" + "/protocol"
)

// gameDomainDirs 是已迁出的游戏域生成包目录（相对仓根）；存在即说明生成物又回流了。
var gameDomainDirs = []string{
	filepath.Join("api", "battle"),
	filepath.Join("api", "game"),
	filepath.Join("api", "matcher"),
}

// gameDomainImportPrefixes 是已迁出的游戏域生成包 import 前缀（SDK 模块内）。
var gameDomainImportPrefixes = []string{
	sdkModule + "/api/battle",
	sdkModule + "/api/game",
	sdkModule + "/api/matcher",
	// 协议包是「游戏侧消费」的独立版本线：SDK 若 require 它，就又把两条版本线绑回一起了。
	protocolModule,
}

// TestNoGameDomainArtifacts 断言仓内不存在游戏域生成包目录（迁移结果的第一层证据）。
func TestNoGameDomainArtifacts(t *testing.T) {
	root := repoRoot(t)
	for _, dir := range gameDomainDirs {
		if _, err := os.Stat(filepath.Join(root, dir)); err == nil {
			t.Errorf("游戏域生成包目录仍在：%s（应由游戏协议包提供，SDK 不再生成）", dir)
		} else if !os.IsNotExist(err) {
			t.Fatalf("检查 %s 失败: %v", dir, err)
		}
	}
}

// TestNoGameDomainImports 断言仓内没有任何 .go 文件 import 游戏域包或协议包
// （含测试与示例：SDK 自己的用例不依赖游戏模块，断言只落在裸字节上）。
func TestNoGameDomainImports(t *testing.T) {
	root := repoRoot(t)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// 跳版本库/依赖目录，以及 CI 检出到工作区内的上游仓（`:!atlas` / `:!atlas-game-layout`，
			// 与 gen-dto 与 gofmt 门禁同款口径）：它们的文件不是本仓产物，不受本守卫约束。
			switch d.Name() {
			case ".git", "node_modules", "atlas", "atlas-game-layout":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, prefix := range gameDomainImportPrefixes {
			if strings.Contains(string(data), `"`+prefix) {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s 仍 import 游戏域包 %s（应改用协议包 + 裸字节，SDK 不持有游戏域消息）", rel, prefix)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历仓内 .go 文件失败: %v", err)
	}
}

// repoRoot 返回本仓根目录（本文件在 direct/ 下，取上一级）。
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位本文件路径")
	}
	return filepath.Dir(filepath.Dir(file))
}
