package exec

import (
	"os/exec"
)

// CmdError is an error with a particular process.
type CmdError struct {
	Cmd *exec.Cmd
	error
}
