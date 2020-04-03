package config

import (
	"testing"
)

func TestTableFilterConfig_00(t *testing.T) {
	filter := TableFilterConfig{
		DbName:  "a",
		Include: []string{"a", "b", "c"},
		Exclude: []string{"m", "n", "c"},
	}

	examples := []struct {
		Input  []string
		Output bool
	}{
		{[]string{"x", "u"}, false},
		{[]string{"a", "a"}, true},
		{[]string{"a", "x"}, false},
		{[]string{"a", "m"}, false},
		{[]string{"a", "c"}, false},
	}
	for _, example := range examples {
		result := filter.IsTablePass(example.Input[0], example.Input[1])
		if result != example.Output {
			t.Errorf("判断错误: input(%v), got(%v), expected(%v)", example.Input, result, example.Output)
		}
	}
}
