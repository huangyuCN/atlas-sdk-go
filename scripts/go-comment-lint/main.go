// go-comment-lint 检查 Go doc 注释是否符合项目规范（docs/go-comments.md），
// 目标：GoLand 的 GoCommentStart（注释首词 ≠ 声明名）与
// GoExportedElementShouldHaveComment（Missing comment）零警告。
//
// 检查项（输出明细的 kind）：
//   - first-word：导出的顶层声明 doc 注释首词 ≠ 声明名（方法注释不允许接收者前缀）
//   - missing：导出的顶层声明缺 doc 注释（_test.go 跳过，与 GoLand 默认行为一致）
//   - detached：注释与声明之间隔了空行（注释未构成 doc 注释）
//   - pkg-form：包注释不以 "Package <包名>" 开头
//   - missing-pkg：非 main 包没有任何包注释
//
// 一律跳过：生成文件（含 "Code generated ... DO NOT EDIT" 标记）、vendor/testdata/
// third_party、点开头目录、`Deprecated:` 注释、结构体/接口字段、行内注释。
//
// 用法：
//
//	go run ./scripts/go-comment-lint [目录]
//
// 目录默认当前目录（递归）。发现违规时输出分类计数与清单，退出码 1。
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// skipDir 是按目录名跳过的集合（不递归进入）。
var skipDir = map[string]bool{
	".git": true, "vendor": true, "testdata": true,
	"third_party": true, "node_modules": true, "bin": true,
}

// violation 是单条违规记录。
type violation struct {
	file   string // 相对根目录的文件路径
	line   int    // 声明所在行（包级为 package 子句行）
	kind   string // first-word / missing / detached / pkg-form / missing-pkg
	name   string // 声明名或包名
	detail string // 实际首词或补充说明
}

var (
	out         []violation
	fset        = token.NewFileSet()
	parsedFiles = map[string]*ast.File{} // 相对路径 → 已解析文件（detached 检查用）
	rootDir     string
)

// exported 判断标识符是否导出（首字符大写）。
func exported(name string) bool {
	r, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(r)
}

// firstWord 返回 doc 注释第一个有内容行（跳过 //go: 指令）的首个空白分隔 token。
func firstWord(doc *ast.CommentGroup) (string, bool) {
	if doc == nil {
		return "", false
	}
	for _, c := range doc.List {
		t := strings.TrimLeft(strings.TrimPrefix(c.Text, "//"), " \t")
		if t == "" || strings.HasPrefix(t, "go:") {
			continue
		}
		if i := strings.IndexAny(t, " \t"); i >= 0 {
			t = t[:i]
		}
		return t, true
	}
	return "", false
}

// isDeprecated 判断注释是否为弃用标记（规则明确不检查 Deprecated 注释）。
func isDeprecated(doc *ast.CommentGroup) bool {
	return doc != nil && strings.Contains(doc.Text(), "Deprecated:")
}

// specName 提取组内声明的名字（类型名或值名首项）。
func specName(s ast.Spec) string {
	switch s := s.(type) {
	case *ast.TypeSpec:
		return s.Name.Name
	case *ast.ValueSpec:
		if len(s.Names) > 0 {
			return s.Names[0].Name
		}
	}
	return ""
}

// specDoc 提取组内声明自身的 doc 注释（可能为 nil）。
func specDoc(s ast.Spec) *ast.CommentGroup {
	switch s := s.(type) {
	case *ast.TypeSpec:
		return s.Doc
	case *ast.ValueSpec:
		return s.Doc
	}
	return nil
}

// checkDecl 按规则检查单个导出声明：缺注释记 missing/detached，首词错记 first-word。
func checkDecl(file, name string, hasDoc bool, pos token.Pos, doc *ast.CommentGroup) {
	if !exported(name) {
		return
	}
	if !hasDoc {
		kind := "missing"
		detail := ""
		if hasDetachedComment(file, pos) {
			kind = "detached"
			detail = "注释与声明之间隔了空行"
		}
		out = append(out, violation{file, fset.Position(pos).Line, kind, name, detail})
		return
	}
	if isDeprecated(doc) {
		return
	}
	w, ok := firstWord(doc)
	if !ok {
		return // 纯指令/空注释
	}
	if w != name && !strings.HasPrefix(w, name+"(") {
		out = append(out, violation{file, fset.Position(pos).Line, "first-word", name, w})
	}
}

// hasDetachedComment 判断声明上方是否隔着空行有注释块（注释结束行 = 声明行-1）。
func hasDetachedComment(file string, pos token.Pos) bool {
	f := parsedFiles[file]
	if f == nil {
		return false
	}
	declLine := fset.Position(pos).Line
	for _, cg := range f.Comments {
		if cg.End() < pos && fset.Position(cg.End()).Line == declLine-1 {
			return true
		}
	}
	return false
}

// scanFile 解析单个 Go 文件并逐声明检查（生成文件直接跳过）。
func scanFile(path, pkgName string, isTest, isMain bool) {
	src, err := os.ReadFile(path)
	if err != nil {
		return
	}
	head := string(src)
	if len(head) > 2048 {
		head = head[:2048]
	}
	if strings.Contains(head, "Code generated") && strings.Contains(head, "DO NOT EDIT") {
		return
	}
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return
	}
	rel, _ := filepath.Rel(rootDir, path)
	parsedFiles[rel] = f

	// 包注释形式检查（测试文件与 main 包跳过）。
	if !isTest && !isMain && f.Doc != nil && !isDeprecated(f.Doc) {
		if w, ok := firstWord(f.Doc); ok {
			toks := strings.Fields(strings.TrimSpace(f.Doc.Text()))
			if w != "Package" || len(toks) < 2 || !strings.HasPrefix(toks[1], pkgName) {
				out = append(out, violation{rel, fset.Position(f.Package).Line, "pkg-form", pkgName, w})
			}
		}
	}

	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if isTest && d.Doc == nil {
				continue // 测试文件跳过 missing（与 GoLand 默认一致）
			}
			checkDecl(rel, d.Name.Name, d.Doc != nil, d.Pos(), d.Doc)
		case *ast.GenDecl:
			if len(d.Specs) == 1 && d.Lparen == token.NoPos {
				if isTest && d.Doc == nil && specDoc(d.Specs[0]) == nil {
					continue // 测试文件跳过 missing（与 GoLand 默认一致）
				}
				doc := d.Doc
				if sd := specDoc(d.Specs[0]); sd != nil {
					doc = sd
				}
				checkDecl(rel, specName(d.Specs[0]), doc != nil, d.Specs[0].Pos(), doc)
				continue
			}
			// 有括号的组：组注释可覆盖成员的 missing；成员自带注释则查首词。
			for _, s := range d.Specs {
				name := specName(s)
				if name == "" {
					continue
				}
				if sd := specDoc(s); sd != nil {
					checkDecl(rel, name, true, s.Pos(), sd)
					continue
				}
				if exported(name) && d.Doc == nil && !isTest {
					out = append(out, violation{rel, fset.Position(s.Pos()).Line, "missing", name, "组内导出成员无注释且组无注释"})
				}
			}
		}
	}
}

// relOf 返回 path 相对根目录的路径（失败时回退原路径）。
func relOf(root, path string) string {
	r, _ := filepath.Rel(root, path)
	return r
}

// quickPkg 从源码首个 package 子句取包名与是否 main。
func quickPkg(src []byte) (string, bool) {
	for _, line := range strings.Split(string(src), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "package ") {
			fields := strings.Fields(line)
			return fields[1], fields[1] == "main"
		}
	}
	return "", false
}

// collectFiles 递归收集根目录下的 .go 文件（跳过隐藏目录与 skipDir）。
func collectFiles(root string) []string {
	var files []string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			name := info.Name()
			if strings.HasPrefix(name, ".") && path != root {
				return filepath.SkipDir
			}
			if skipDir[name] && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			files = append(files, path)
		}
		return nil
	})
	sort.Strings(files)
	return files
}

// main 扫描根目录并输出违规清单；有违规时退出码 1。
func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	rootDir = root
	files := collectFiles(root)

	pkgNameOf := map[string]string{} // 目录 → 包名
	isMainPkg := map[string]bool{}   // 目录 → 是否 main 包
	for _, path := range files {
		isTest := strings.HasSuffix(path, "_test.go")
		src, _ := os.ReadFile(path)
		pkgName, isMain := quickPkg(src)
		dir := filepath.Dir(path)
		if !isTest {
			pkgNameOf[dir] = pkgName
			if isMain {
				isMainPkg[dir] = true
			}
		}
		scanFile(path, pkgName, isTest, isMain)
	}

	// missing-pkg：非 main 包（至少一个非测试文件）没有任何包注释。
	for dir, pkgName := range pkgNameOf {
		if isMainPkg[dir] || pkgName == "" {
			continue
		}
		hasDoc := false
		for _, path := range files {
			if filepath.Dir(path) != dir || strings.HasSuffix(path, "_test.go") {
				continue
			}
			if f := parsedFiles[relOf(root, path)]; f != nil && f.Doc != nil && !isDeprecated(f.Doc) {
				hasDoc = true
				break
			}
		}
		if !hasDoc {
			rel, _ := filepath.Rel(root, dir)
			out = append(out, violation{rel, 0, "missing-pkg", pkgName, "包无任何包注释"})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].file != out[j].file {
			return out[i].file < out[j].file
		}
		return out[i].line < out[j].line
	})
	byKind := map[string]int{}
	for _, v := range out {
		byKind[v.kind]++
	}
	for _, k := range []string{"pkg-form", "missing-pkg", "first-word", "detached", "missing"} {
		fmt.Printf("== %-12s %d\n", k, byKind[k])
	}
	for _, v := range out {
		fmt.Printf("%s:%d [%s] %s %s\n", v.file, v.line, v.kind, v.name, v.detail)
	}
	if len(out) > 0 {
		os.Exit(1)
	}
}
