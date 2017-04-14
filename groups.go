package exec

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/pkg/errors"
)

const (
	processFile = "process.json"
	stdoutFile  = ".stdout"
	stderrFile  = ".stderr"
	dotCurrent  = ".current"
)

// Groups manages a collection of Group's by persisting group information to disk.
// A group of processes is represented as a set of directories whose names are the PID's of the processes.
//
// Each directory contains the following files:
// * process.json is used to persist the necessary information to run the command, e.g. command args, environment variables
// * .stdout and .stderr contain whatever the process has output on stdout and stderr
// * .current will exist in the directory of the group that is the currently open group
//
type Groups struct {
	// cur is the currently open process group.
	cur *Group

	// fd is the *os.File for the root directory.
	fd *os.File

	// root is root directory of the Groups.
	root string
}

// NewGroups creates
func NewGroups(root string) (*Groups, error) {
	g := &Groups{
		root: root,
	}
	if err := g.initialize(); err != nil {
		return nil, errors.Wrap(err, "initializing groups")
	}
	return g, nil
}

// Close closes the current Group.
func (g *Groups) Close() error {
	if g.cur == nil {
		return nil
	}
	return g.cur.Signal(syscall.SIGKILL)
}

// Current returns the current Group.
func (g *Groups) Current() *Group {
	return g.cur
}

func (g *Groups) initialize() error {
	fd, err := openOrCreateDir(g.root)
	if err != nil {
		return errors.Wrap(err, "initializing root dir")
	}
	g.fd = fd

	if err := g.openExisting(); err != nil {
		return errors.Wrap(err, "opening existing groups")
	}
	return nil
}

// Open opens the Group with the provided name and sets it to the current Group.
func (g *Groups) Open(name string) error {
	return nil
}

// openExisting opens the existing groups rooted at g.root
func (g *Groups) openExisting() error {
	dirs, err := g.fd.Readdir(-1)
	if err != nil {
		return errors.Wrap(err, "reading directory")
	}
	for _, dir := range dirs {
		if err := g.openGroupFrom(dir); err != nil {
			return errors.Wrap(err, "opening group from "+dir.Name())
		}
	}
	return nil
}

// openGroupFrom opens a group from the provided directory and adds it to the Groups.
func (g *Groups) openGroupFrom(info os.FileInfo) error {
	if !info.IsDir() {
		return errors.New(info.Name() + " is not a directory")
	}
	// The directory should be located in the root directory of the Groups.
	gpath := filepath.Join(g.root, info.Name())
	groupDir, err := os.Open(gpath)
	if err != nil {
		return errors.Wrap(err, "opening group directory")
	}
	processDirs, err := groupDir.Readdir(-1)
	if err != nil {
		return errors.Wrap(err, "reading files in directory")
	}
	group := NewGroup()
	for _, processInfo := range processDirs {
		existingProcPath := filepath.Join(gpath, processInfo.Name())
		if err := renewProcessFrom(gpath, existingProcPath, group); err != nil {
			return err
		}
	}
	// If dotCurrent exists in the group directory then make it the current group.
	if _, err := os.Open(filepath.Join(gpath, dotCurrent)); !os.IsNotExist(err) {
		g.cur = group
	}
	return nil
}

const dirPerms = 0755

// openOrCreateDir opens a directory with the provided path,
// and creates it if it doesn't exist
func openOrCreateDir(dirpath string) (*os.File, error) {
	fd, err := os.Open(dirpath)
	if err == nil {
		return fd, nil
	}
	// Bail if it exists but we can't open it.
	if !os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "opening %s", dirpath)
	}
	// Create and open the directory because it doesn't exist.
	if err := os.Mkdir(dirpath, dirPerms); err != nil {
		return nil, errors.Wrap(err, "making directory")
	}
	return os.Open(dirpath)
}

// renewProcessFrom reads info about an old process from the directory at procPath,
// starts a new one, and adds it to the provided group.
func renewProcessFrom(gpath, existingProcPath string, group *Group) error {
	// Decode the existing process file and start a new process.
	procFile, err := os.Create(filepath.Join(existingProcPath, processFile))
	if err != nil {
		return errors.Wrap(err, "creating new process file")
	}
	cmdenc := command{}
	if err := json.NewDecoder(procFile).Decode(&cmdenc); err != nil {
		return errors.Wrap(err, "decoding "+processFile)
	}
	// Args should always have the command name at index 0
	cmd := exec.Command(cmdenc.Path, cmdenc.Args[1:]...)

	// TODO: will we need to modify the env of the new process?
	cmd.Env = cmdenc.Env

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "getting stdout pipe")
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "getting stderr pipe")
	}
	if err := group.Start(cmd); err != nil {
		return err
	}
	procPath := filepath.Join(gpath, strconv.Itoa(cmd.Process.Pid))
	if err := os.Mkdir(procPath, dirPerms); err != nil {
		return errors.Wrap(err, "making new process directory")
	}
	// Pipe stdout and stderr.
	stdout, err := os.Create(filepath.Join(procPath, stdoutFile))
	if err != nil {
		return errors.Wrap(err, "creating new process stdout file")
	}
	stderr, err := os.Create(filepath.Join(procPath, stderrFile))
	if err != nil {
		return errors.Wrap(err, "creating new process stderr file")
	}
	go func() { _, _ = io.Copy(stdout, stdoutPipe) }()
	go func() { _, _ = io.Copy(stderr, stderrPipe) }()

	// TODO: when to clean out the old process directories?

	// Write the new process file.
	return json.NewEncoder(procFile).Encode(command{
		Path: cmd.Path,
		Args: cmd.Args,
		Env:  cmd.Env,
	})
}

// command is a utility type used to encode/decode commands.
type command struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
	Path string   `json:"path"`
}
