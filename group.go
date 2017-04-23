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

// Remove removes commands from a group.
// Any commands with the provided ID's are stopped and removed from the group.
func (g *Group) Remove(commandIDs ...string) error {
	var (
		cmds = []*exec.Cmd{}
		cm   = map[string]struct{}{}
	)
	for _, cid := range commandIDs {
		cm[cid] = struct{}{}
	}
	for _, cmd := range g.cmds {
		cid, err := GetCmdID(cmd)
		if err != nil {
			return errors.Wrap(err, "getting command ID")
		}
		if _, ok := cm[cid]; ok {
			if err := g.Stop(cmd.Process.Pid); err != nil {
				return errors.Wrapf(err, "stopping process PID=%d", cmd.Process.Pid)
			}
			continue
		}
		cmds = append(cmds, cmd)
	}
	g.cmds = cmds
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

// Stop stops a process.
func (g *Group) Stop(pid int) error {
	var (
		cmd  *exec.Cmd
		cmds = []*exec.Cmd{}
	)
	for _, cc := range g.cmds {
		if cc.Process.Pid == pid {
			cmd = cc
			continue
		}
		cmds = append(cmds, cc)
	}
	if cmd == nil {
		return errors.Errorf("process %d not found", pid)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		if isAlreadyFinished(err) {
			return nil // The process is already finished.
		}
		return errors.Wrap(err, "sending kill signal")
	}
	if err := cmd.Wait(); err != nil {
		return errors.Wrap(err, "waiting for process to finish")
	}
	g.cmds = cmds
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
