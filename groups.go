package exec

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/pkg/errors"
)

const (
	processFile = "process.json"
	stdoutFile  = ".stdout"
	stderrFile  = ".stderr"
	dotCurrent  = ".current"
	dirPerms    = 0755
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
	cur string

	// fd is the *os.File for the root directory.
	fd *os.File

	// groups maps base names of group directories to groups.
	groups   map[string]*Group
	groupsMu sync.RWMutex

	// root is root directory of the Groups.
	root string
}

// NewGroups creates a new collection of persistent process groups.
func NewGroups(root string) (*Groups, error) {
	g := &Groups{
		groups: map[string]*Group{},
		root:   root,
	}
	if err := g.initialize(); err != nil {
		return nil, errors.Wrap(err, "initializing groups")
	}
	return g, nil
}

// Close closes the current Group.
func (g *Groups) Close() error {
	errs := []string{}

	if cur := g.Current(); cur != nil {
		if err := cur.Signal(syscall.SIGKILL); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if err := g.fd.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, ", and "))
}

// Current returns the current Group.
func (g *Groups) Current() *Group {
	g.groupsMu.RLock()
	cur := g.groups[g.cur]
	g.groupsMu.RUnlock()
	return cur
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

// initializeGroup initializes a new group and makes it the current group.
func (g *Groups) initializeGroup(name string) error {
	g.groups[name] = NewGroup()

	// Initialize the directory for the new group.
	if err := os.Mkdir(filepath.Join(g.root, name), dirPerms); err != nil {
		return errors.Wrap(err, "making group directory")
	}
	if err := touch(filepath.Join(g.root, name, dotCurrent)); err != nil {
		return errors.Wrap(err, "creating "+dotCurrent)
	}
	return g.setCurrent(name)
}

// Open opens the Group with the provided name and sets it to the current Group.
// If there is no Group with the provided name then this method initializes a new one.
func (g *Groups) Open(name string) error {
	g.groupsMu.Lock()
	defer g.groupsMu.Unlock()

	group, ok := g.groups[name]
	if !ok {
		return g.initializeGroup(name)
	}
	if err := group.Signal(syscall.SIGKILL); err != nil {
		return errors.Wrap(err, "sending SIGKILL to current process group")
	}
	info, err := os.Stat(filepath.Join(g.root, name))
	if err != nil {
		return err
	}
	if err := g.openGroupFrom(info); err != nil {
		return err
	}
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
	if _, err := os.Stat(filepath.Join(gpath, dotCurrent)); !os.IsNotExist(err) {
		g.cur = info.Name()
	}
	g.groupsMu.Lock()
	g.groups[info.Name()] = group
	g.groupsMu.Unlock()
	return nil
}

// setCurrent sets the current group.
func (g *Groups) setCurrent(name string) error {
	// Early out if there is no current group.
	if g.cur == "" {
		g.cur = name
		return nil
	}
	// Remove dotCurrent from the current group
	if err := os.Remove(filepath.Join(g.root, g.cur, dotCurrent)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	g.cur = name
	return nil
}

// Start starts a process in the current group.
func (g *Groups) Start(cmd *exec.Cmd) error {
	cur := g.Current()
	if cur == nil {
		return errors.New("no current group")
	}
	return cur.Start(cmd)
}

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

// touch touches a file.
func touch(name string) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	return f.Close()
}

// command is a utility type used to encode/decode commands.
type command struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
	Path string   `json:"path"`
}
