package exec

import (
	"os"
	"time"

	"github.com/pkg/errors"
)

// Pid is a process ID.
type Pid int

// Group runs a set of commands.
type Group struct {
	cmds   []*Cmd
	done   chan *Cmd
	errors chan CmdError
}

// NewGroup creates a new Group instance.
// ctx can be used to cancel the entire group of processes.
func NewGroup() *Group {
	return &Group{
		cmds:   []*Cmd{},
		done:   make(chan *Cmd),
		errors: make(chan CmdError),
	}
}

// Commands returns the commands associated with the Group.
func (g *Group) Commands() []*Cmd {
	return g.cmds
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
func (g *Group) Start(cmd *Cmd) error {
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
