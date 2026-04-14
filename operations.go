package kwality

import "os"

func ensureDir(dirPath string) error {
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}
	return nil
}
