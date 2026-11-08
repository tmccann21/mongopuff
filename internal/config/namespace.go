package config

import (
	"errors"
	"fmt"
	"strings"
)

// maxNamespaceLen is the longest namespace name turbopuffer accepts.
const maxNamespaceLen = 128

// Namespace is the turbopuffer namespace from config: either a fixed name or a
// template whose {{field}} tokens are filled in from each document.
type Namespace struct {
	raw string
	// parts is the template split into literal text and {{field}} tokens, in
	// order. It is nil for a static namespace.
	parts []namespacePart
}

// namespacePart is literal text, or the field name inside a {{field}} token.
type namespacePart struct {
	text    string
	isField bool
}

// ParseNamespace parses s as a fixed namespace name or a template containing
// top-level {{field}} tokens. It rejects a template that can never resolve to
// a valid name, so the mistake fails once at startup instead of once per event
// in the DLQ.
func ParseNamespace(s string) (Namespace, error) {
	parts, err := parseNamespaceParts(s)
	if err != nil {
		return Namespace{}, fmt.Errorf("namespace %q: %w", s, err)
	}
	return Namespace{raw: s, parts: parts}, nil
}

// MustParseNamespace panics if ParseNamespace fails.
func MustParseNamespace(s string) Namespace {
	n, err := ParseNamespace(s)
	if err != nil {
		panic(err)
	}
	return n
}

// parseNamespaceParts splits s into its parts, or returns nil for a static
// namespace. It rejects a template that can never resolve to a valid name, so
// the mistake fails once at startup instead of once per event in the DLQ.
func parseNamespaceParts(s string) ([]namespacePart, error) {
	var parts []namespacePart
	var literals strings.Builder
	dynamic := false
	for rest := s; rest != ""; {
		literal, token, found := strings.Cut(rest, "{{")
		if strings.Contains(literal, "}}") {
			return nil, errors.New(`"}}" has no matching "{{"`)
		}
		if literal != "" {
			parts = append(parts, namespacePart{text: literal})
			literals.WriteString(literal)
		}
		if !found {
			break
		}

		field, after, found := strings.Cut(token, "}}")
		if !found {
			return nil, errors.New(`"{{" has no matching "}}"`)
		}
		if err := checkTemplateField(field); err != nil {
			return nil, err
		}
		parts = append(parts, namespacePart{text: field, isField: true})
		dynamic = true
		rest = after
	}

	if !dynamic {
		return nil, checkNamespaceName(s)
	}

	// The literal text is part of every resolved name.
	if err := checkNamespaceChars(literals.String()); err != nil {
		return nil, err
	}
	if n := literals.Len(); n > maxNamespaceLen {
		return nil, fmt.Errorf("text outside {{field}} tokens is %d characters; the limit is %d", n, maxNamespaceLen)
	}
	return parts, nil
}

func checkTemplateField(field string) error {
	switch {
	case field == "":
		return errors.New("{{}} names no field")
	case strings.Contains(field, "."):
		return fmt.Errorf("nested field path %q is not supported", field)
	}
	return nil
}

// IsDynamic reports whether the namespace contains a {{field}} token.
func (n Namespace) IsDynamic() bool {
	return n.parts != nil
}

// Resolve returns the namespace name for one document, calling value to get
// each {{field}}. A static namespace returns its raw string without calling
// value. The resolved name is checked against turbopuffer's naming rules, so
// Resolve can fail even when every value call succeeds.
func (n Namespace) Resolve(value func(field string) (string, error)) (string, error) {
	if !n.IsDynamic() {
		return n.raw, nil
	}

	var b strings.Builder
	for _, p := range n.parts {
		if !p.isField {
			b.WriteString(p.text)
			continue
		}
		v, err := value(p.text)
		if err != nil {
			return "", fmt.Errorf("namespace field %q: %w", p.text, err)
		}
		b.WriteString(v)
	}

	name := b.String()
	if err := checkNamespaceName(name); err != nil {
		return "", fmt.Errorf("resolved namespace %q: %w", name, err)
	}
	return name, nil
}

// String returns the raw namespace string.
func (n Namespace) String() string {
	return n.raw
}

// MarshalText implements encoding.TextMarshaler. It returns the raw string.
func (n Namespace) MarshalText() ([]byte, error) {
	return []byte(n.raw), nil
}

// UnmarshalText implements encoding.TextUnmarshaler using ParseNamespace, so
// an invalid template fails when the config is loaded.
func (n *Namespace) UnmarshalText(text []byte) error {
	parsed, err := ParseNamespace(string(text))
	if err != nil {
		return err
	}
	*n = parsed
	return nil
}

// checkNamespaceName reports whether turbopuffer accepts name, which must
// match [A-Za-z0-9-_.]{1,128}.
func checkNamespaceName(name string) error {
	if name == "" {
		return errors.New("name is empty")
	}
	if err := checkNamespaceChars(name); err != nil {
		return err
	}
	if len(name) > maxNamespaceLen {
		return fmt.Errorf("name is %d characters; the limit is %d", len(name), maxNamespaceLen)
	}
	return nil
}

func checkNamespaceChars(s string) error {
	for _, r := range s {
		switch {
		case 'A' <= r && r <= 'Z', 'a' <= r && r <= 'z', '0' <= r && r <= '9', r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("%q is not allowed; a name may contain only A-Z, a-z, 0-9, '-', '_' and '.'", r)
		}
	}
	return nil
}
