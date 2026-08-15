//go:build windows

package recorder

import "os/exec"

func setProcessGroup(cmd *exec.Cmd) {}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}

// interruptProcessGroup has no workable counterpart here. Go's os.Process.Signal
// implements only Kill on Windows, and delivering a console CTRL event to a child
// requires it to share this process's console -- which the recorder's ffmpeg does
// not. Shutdown therefore keeps relying on the stdin "q" and, failing that, the
// force-kill; the truncation that follows is survivable because the segments are
// fragmented MP4 (see the segment_format_options in recorder.go).
func interruptProcessGroup(cmd *exec.Cmd) error {
	return nil
}
