package file

import (
	"os"
)

func ReadExpandEnv(in []byte) []byte {
	return []byte(os.ExpandEnv(string(in)))
}
