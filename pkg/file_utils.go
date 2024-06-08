package pkg

import "os"

func FileExisted(name string) bool {
	_, err := os.Stat(name)

	return err == nil
}
