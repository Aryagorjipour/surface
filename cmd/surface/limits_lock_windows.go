//go:build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

func lockFile(f *os.File) error {
	ol := new(syscall.Overlapped)
	r, _, err := procLockFileEx.Call(
		f.Fd(),
		uintptr(0x2),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(ol)),
	)
	if r == 0 {
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	ol := new(syscall.Overlapped)
	r, _, err := procUnlockFileEx.Call(
		f.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(ol)),
	)
	if r == 0 {
		return err
	}
	return nil
}
