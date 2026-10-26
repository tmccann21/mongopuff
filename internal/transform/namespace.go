package transform

import (
	"fmt"
	"regexp"
	"strings"
)

var tokenRe = regexp.MustCompile(`\{\{([^}]+)\}\}`)

func IsDynamic(namespace string) bool {
	return strings.Contains(namespace, "{{")
}

func ResolveNamespace(template string, doc map[string]any) (string, error) {
	matches := tokenRe.FindAllStringSubmatchIndex(template, -1)
	if len(matches) == 0 {
		return template, nil
	}

	var sb strings.Builder
	pos := 0
	for _, m := range matches {
		sb.WriteString(template[pos:m[0]])
		field := template[m[2]:m[3]]
		if strings.Contains(field, ".") {
			return "", fmt.Errorf("namespace template: nested field path %q is not supported", field)
		}
		val, ok := doc[field]
		if !ok || val == nil {
			return "", fmt.Errorf("namespace field %q is absent or null", field)
		}
		sb.WriteString(fmt.Sprint(val))
		pos = m[1]
	}
	sb.WriteString(template[pos:])
	return sb.String(), nil
}
