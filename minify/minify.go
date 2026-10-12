package minify

import (
	"strings"
	"unicode"
)

func Minify(s string) string {
	sql := []rune(strings.TrimSpace(s))
	length := len(sql)

	var result []rune
	var quote rune
	whitespace := false
	comment := false
	multiline := false

	for i := 0; i < length; i++ {
		char := sql[i]

		// --- Comments ---
		if !comment && !multiline && char == '-' && i+1 < length && sql[i+1] == '-' {
			comment = true
			i++ // skip next
			continue
		}
		if !comment && char == '/' && i+1 < length && sql[i+1] == '*' {
			comment = true
			multiline = true
			i++
			continue
		}
		if comment && multiline && char == '*' && i+1 < length && sql[i+1] == '/' {
			comment = false
			multiline = false
			i++
			continue
		}
		if comment && !multiline && (char == '\n' || char == '\r') {
			comment = false
			continue
		}
		if comment {
			continue
		}

		// --- Newlines and tabs ---
		if char == '\n' || char == '\r' || char == '\t' {
			whitespace = true
			continue
		}

		// --- Multiple spaces ---
		if unicode.IsSpace(char) {
			whitespace = true
			continue
		}

		if whitespace {
			if len(result) > 0 && result[len(result)-1] != ' ' {
				result = append(result, ' ')
			}
			whitespace = false
		}

		// --- Remove backticks ---
		if char == '`' {
			continue
		}

		// --- Handle quotes (strings) ---
		if quote == 0 && (char == '\'' || char == '"') {
			quote = char
			result = append(result, char)
			continue
		} else if quote > 0 {
			result = append(result, char)
			if char == quote {
				quote = 0
			}
			continue
		}

		result = append(result, char)
	}

	// Remove trailing space if it exists
	if len(result) > 0 && result[len(result)-1] == ' ' {
		result = result[:len(result)-1]
	}

	return string(result)
}
