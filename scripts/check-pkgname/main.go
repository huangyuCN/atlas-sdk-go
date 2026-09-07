// check-pkgname 检查包内导出标识符（函数/类型/变量/方法）是否以包名开头。
//
// 依据 AGENTS.md 命名规范：调用侧在使用时本身带有包名限定（如 session.NewManager），
// 若导出名再以包名开头将形成冗余（session.SessionNewManager），且会触发 IDE 警告
// "Name starts with the package name"。正确做法：session.NewManager（而非
// session.SessionNewManager）。
//
// 用法：
//
//	go run ./scripts/check-pkgname [目录] [豁免文件] [排除目录...]
//
// 目录默认当前目录，递归遍历 .go 文件，排除 _test.go / .pb.go / third_party。
// 排除目录为相对路径前缀（如 atlas/ 表示跳过 CI 检出的依赖源码树），可传多个。
// 豁免文件每行一条「相对路径:标识符」（如 log/logger.go:Logger），记录经人工
// 裁断确认的非违规项（完整词惯例类型名、跨包接口实现方法等）；命中豁免的
// 标识符不再报告。违规时输出清单并退出码 1。
package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	var allow []string
	if len(os.Args) > 2 {
		allow = readAllow(os.Args[2])
	}
	excludes := os.Args[3:]
	found := false
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || !strings.HasSuffix(path, ".go") {
			return nil
		}
		if info.IsDir() {
			for _, ex := range excludes {
				if strings.HasPrefix(path+string(filepath.Separator), ex) {
					return filepath.SkipDir
				}
			}
			return nil
		}
		for _, ex := range excludes {
			if strings.HasPrefix(path, ex) {
				return nil
			}
		}
		if strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, ".pb.go") ||
			strings.Contains(path, string(filepath.Separator)+"third_party"+string(filepath.Separator)) {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return nil
		}
		prefix := pkgPrefix(f.Name.Name)
		check := func(name, kind string, pos token.Pos) {
			if !ast.IsExported(name) || prefix == "" {
				return
			}
			// 类型名恰等于包名主题（session.Session、config.Config）是 Go 生态惯例，豁免。
			if name == prefix {
				return
			}
			// 需要比包名前缀长才有意义（完全同名属 Go 语法不允许，不会出现）。
			if len(name) > len(prefix) && strings.HasPrefix(name, prefix) &&
				!allowed(allow, path, name) {
				found = true
				fmt.Printf("%s:%d: %s %s 以包名 %q 开头\n",
					path, fset.Position(pos).Line, kind, name, f.Name.Name)
			}
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				check(d.Name.Name, "func", d.Name.Pos())
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						check(s.Name.Name, "type", s.Name.Pos())
					case *ast.ValueSpec:
						for _, n := range s.Names {
							check(n.Name, "var/const", n.Pos())
						}
					}
				}
			}
		}
		return nil
	})
	if found {
		os.Exit(1)
	}
}

// readAllow 读取豁免文件（每行一条「路径:标识符」，# 开头为注释）。
func readAllow(p string) []string {
	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// allowed 判断 path:name 是否在豁免列表中（路径匹配用 strings.Contains 前缀宽松匹配）。
func allowed(allow []string, path, name string) bool {
	if len(allow) == 0 {
		return false
	}
	for _, a := range allow {
		parts := strings.SplitN(a, ":", 2)
		if len(parts) != 2 {
			continue
		}
		apath, aname := parts[0], parts[1]
		if aname == name && strings.HasSuffix(path, apath) {
			return true
		}
	}
	return false
}

// pkgPrefix 把包名转为首字母大写的驼峰前缀（session → Session）。
func pkgPrefix(pkg string) string {
	if pkg == "" {
		return ""
	}
	return strings.ToUpper(pkg[:1]) + pkg[1:]
}
