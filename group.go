package exec

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

// Group runs a set of commands.
type Group struct {
	cmds   []*exec.Cmd
	done   chan *exec.Cmd
	errors chan CmdError
}

// NewGroup creates a new Group instance.
// ctx can be used to cancel the entire group of processes.
func NewGroup() *Group {
	return &Group{
		cmds:   []*exec.Cmd{},
		done:   make(chan *exec.Cmd),
		errors: make(chan CmdError),
	}
}

// Commands returns the commands associated with the Group.
func (g *Group) Commands() []*exec.Cmd {
	return g.cmds
}

// Remove removes processes from a Group.
// If no PID's are passed this method stops all the processes in the group.
func (g *Group) Remove(cmds ...*exec.Cmd) error {
	var (
		pm       = map[int]struct{}{}
		errs     = []string{}
		errch    = make(chan error)
		done     = make(chan struct{})
		stopping []*exec.Cmd
	)
	if len(cmds) == 0 {
		stopping = g.cmds
	} else {
		stopping = cmds
	}
	for _, cmd := range stopping {
		pm[cmd.Process.Pid] = struct{}{}

		go func(cmd *exec.Cmd) {
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				if isAlreadyFinished(err) {
					done <- struct{}{} // The process is already finished.
					return
				}
				errch <- errors.Wrap(err, "sending kill signal")
			}
			if err := cmd.Wait(); err != nil {
				errch <- errors.Wrap(err, "waiting for process to finish")
			}
		}(cmd)
	}
	for range pm {
		select {
		case <-time.After(2 * time.Second):
			return errors.New("timeout")
		case <-done:
		case err := <-errch:
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", and "))
	}
	newCmds := []*exec.Cmd{}

	for _, cc := range g.cmds {
		if _, ok := pm[cc.Process.Pid]; ok {
			continue
		}
		newCmds = append(newCmds, cc)
	}
	g.cmds = newCmds
	return nil
}

// Signal sends a signal to every process in the Group.
func (g *Group) Signal(signal os.Signal) error {
	for _, cmd := range g.cmds {
		if err := cmd.Process.Signal(signal); err != nil {
			return err
		}
	}
	return nil
}

// Start starts the provided command and adds it to the group.
// It also starts a goroutine that waits for the command.
func (g *Group) Start(cmd *exec.Cmd) error {
	// Start the process.
	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "starting command")
	}
	go func() {
		if err := cmd.Wait(); err != nil {
			g.errors <- CmdError{
				Cmd:   cmd,
				error: err,
			}
			return
		}
		g.done <- cmd
	}()
	g.cmds = append(g.cmds, cmd)
	return nil
}

// Wait waits for all commands to finish.
// If there was an error running any of the commands then CmdError will be returned.
func (g *Group) Wait(timeout time.Duration) error {
	for range g.cmds {
		select {
		case <-time.After(timeout):
			return errors.New("timeout after " + timeout.String())
		case <-g.done:
			// Finished without a problem.
		case cmderr := <-g.errors:
			return cmderr
		}
	}
	return nil
}

func isAlreadyFinished(err error) bool {
	return strings.HasSuffix(err.Error(), "process already finished")
}
