package reflow

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// abbrev abbreviates string s to a length of max.
func abbrev(s string, max int) string {
	if len(s) <= max {
		return s
	}
	a := make([]byte, 0, max)
	a = append(a, s[:max/2-1]...)
	a = append(a, ".."...)
	a = append(a, s[len(s)-max/2+1:]...)
	return string(a)
}

// leftabbrev abbreviates string s by retaining its rightmost 'max'
// characters.
func leftabbrev(s string, max int) string {
	if len(s) <= max {
		return s
	}
	a := make([]byte, 0, max)
	a = append(a, ".."...)
	a = append(a, s[len(s)-max+2:]...)
	return string(a)
}

// trim returns a version of s where whitespace characters, as
// defined by unicode.IsSpace, are replaced by ".". Space (" ") is
// treated specially; it is allowed to remain. Adjacent whitespaces
// are collapsed; leading and trailing whitespace is dropped
// alltogether.
func trimspace(s string) string {
	s = strings.TrimSpace(s)
	buf := make([]byte, 0, len(s))
	var wasspace bool
	for width := 0; len(s) > 0; s = s[width:] {
		r := rune(s[0])
		width = 1
		if r >= utf8.RuneSelf {
			r, width = utf8.DecodeRuneInString(s)
		}
		space := unicode.IsSpace(r)
		if space {
			if !wasspace {
				switch r {
				case ' ':
					buf = append(buf, " "...)
				default:
					buf = append(buf, "."...)
				}
			}
		} else {
			buf = append(buf, s[:width]...)
		}
		wasspace = space
	}
	return string(buf)
}

// trimpath trims a leading absolute path name, replacing all but its
// base with ".."
func trimpath(s string) string {
	u := s
	for len(s) > 0 && s[0] == '/' {
		i := strings.Index(s[1:], "/")
		if i < 0 || strings.IndexFunc(s[1:i], unicode.IsSpace) >= 0 {
			if s == "/" {
				s = u
			}
			return ".." + s
		}
		s, u = s[i+1:], s
	}
	return s
}
