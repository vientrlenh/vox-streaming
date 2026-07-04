//go:build windows

package util

import "golang.org/x/sys/windows"

func AvailableDiskSpace(dir string) (uint64, error) {
	pathPtr, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, err
	}
	var freeBytesAvailable uint64
	if err := windows.GetDiskFreeSpaceEx(pathPtr, &freeBytesAvailable, nil, nil); err != nil {
		return 0, err
	}
	return freeBytesAvailable, nil
}