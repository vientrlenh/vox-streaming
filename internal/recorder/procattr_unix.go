//go:build !windows

package recorder

import (
	"os/exec"
	"syscall"
)


func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// interruptProcessGroup asks ffmpeg to stop the way Ctrl+C does: unwind the
// transcode loop and write the trailer, leaving the segment it was mid-write on
// readable.
//
// This is what actually reaches a blocked ffmpeg. libavformat polls its interrupt
// callback while waiting on the network, and that callback reports ffmpeg's own
// received_sigterm -- so SIGINT breaks the read that the stdin "q" can never get
// past.
func interruptProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
}
