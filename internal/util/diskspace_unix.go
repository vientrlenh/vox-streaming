//go:build !windows

package util

import "golang.org/x/sys/unix"

func AvailableDiskSpace(dir string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}