package file

import (
	"os"
	"regexp"
)

func FindEnvVars(in []byte) (out []string) {
	re := regexp.MustCompile(`\$\{([A-Za-z0-9_]*)\}`)
	matches := re.FindAllStringSubmatch(string(in), -1)

	for _, m := range matches {
		out = append(out, m[1])
	}

	return out
}

func ReadExpandEnv(in []byte) []byte {
	return []byte(os.ExpandEnv(string(in)))
}
