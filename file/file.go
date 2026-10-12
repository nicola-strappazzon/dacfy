package file

import (
	"os"
	"regexp"
)

var envVarWithBraces = regexp.MustCompile(`\{(\$\{[A-Za-z0-9_]+\}|\$[A-Za-z_][A-Za-z0-9_]*)\}`)

func FindEnvVars(in []byte) (out []string) {
	re := regexp.MustCompile(`\$\{([A-Za-z0-9_]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)
	matches := re.FindAllStringSubmatch(string(in), -1)

	for _, m := range matches {
		if m[1] != "" {
			out = append(out, m[1])
		} else {
			out = append(out, m[2])
		}
	}

	return out
}

func ReadExpandEnv(in []byte) []byte {
	value := envVarWithBraces.ReplaceAllString(string(in), "$1")
	return []byte(os.ExpandEnv(value))
}
