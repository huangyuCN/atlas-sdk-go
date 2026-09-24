package frame

import "testing"

// TestParseClassAcceptsNameAndNumber 断言错误分类的两种文本口径都能解析：protojson
// 枚举名（框架 errors.proto 的 ErrorClass 枚举名 ERROR_CLASS_*）、框架标签与十进制
// 数值（枚举化前的 int32 形态）；未知文本回落未分类，不 panic。
func TestParseClassAcceptsNameAndNumber(t *testing.T) {
	cases := map[string]Class{
		"ERROR_CLASS_UNSPECIFIED": ClassUnspecified,
		"CLASS_UNSPECIFIED":       ClassUnspecified,
		"unspecified":             ClassUnspecified,
		"":                        ClassUnspecified,
		"ERROR_CLASS_BUSINESS":    ClassBusiness,
		"CLASS_BUSINESS":          ClassBusiness,
		"business":                ClassBusiness,
		"1":                       ClassBusiness,
		"ERROR_CLASS_RUNTIME":     ClassRuntime,
		"CLASS_RUNTIME":           ClassRuntime,
		"runtime":                 ClassRuntime,
		"2":                       ClassRuntime,
		"ERROR_CLASS_CANCELED":    ClassCanceled,
		"ERROR_CLASS_CANCELLED":   ClassCanceled,
		"CLASS_CANCELED":          ClassCanceled,
		"canceled":                ClassCanceled,
		"3":                       ClassCanceled,
		"??":                      ClassUnspecified,
	}
	for text, want := range cases {
		if got := ParseClass(text); got != want {
			t.Errorf("ParseClass(%q) = %d, 期望 %d", text, got, want)
		}
	}
}

// TestClassString 断言分类标签与框架 errors.Class 同口径（日志与诊断用）。
func TestClassString(t *testing.T) {
	cases := map[Class]string{
		ClassUnspecified: "unspecified",
		ClassBusiness:    "business",
		ClassRuntime:     "runtime",
		ClassCanceled:    "canceled",
		Class(99):        "unspecified",
	}
	for class, want := range cases {
		if got := class.String(); got != want {
			t.Errorf("Class(%d).String() = %q, 期望 %q", class, got, want)
		}
	}
}
