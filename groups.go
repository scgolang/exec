package exec

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3" // Load sqlite driver.
	"github.com/pkg/errors"
)

// DirPerms are permissions for directories created by this package.
const DirPerms = 0755

// Cmd is a command with an ID.
// The ID should be unique per process group.
type Cmd struct {
	*exec.Cmd
	ID string
}

// CmdError is an error with a particular process.
type CmdError struct {
	Cmd *Cmd
	error
}

// Groups manages a collection of Group's by persisting group information to disk.
type Groups struct {
	// groups is a map from group name to Group.
	groups   map[string]*Group
	groupsMu sync.RWMutex

	// db is a database handle.
	db *sql.DB

	// root is the root directory of the groups.
	root string
}

// NewGroups creates a new collection of persistent process groups.
func NewGroups(root, dbfile string) (*Groups, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	g := &Groups{
		groups: map[string]*Group{},
		root:   absRoot,
	}
	info, err := os.Stat(g.root)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(g.root, DirPerms); err != nil {
				return nil, errors.Wrap(err, "creating "+g.root+" directory")
			}
		}
	}
	if info != nil && !info.IsDir() {
		return nil, errors.Wrap(err, g.root+" is not a directory")
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, dbfile))
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}
	g.db = db
	if err := g.initialize(); err != nil {
		return nil, errors.Wrap(err, "initializing groups")
	}
	return g, nil
}

// captureOutput captures the output of the provided command.
func (g *Groups) captureOutput(outPipe, errPipe io.ReadCloser, commandID string) error {
	stdout, err := os.Create(filepath.Join(g.root, fmt.Sprintf("%s.stdout", commandID)))
	if err != nil {
		return errors.Wrap(err, "creating new process stdout file")
	}
	stderr, err := os.Create(filepath.Join(g.root, fmt.Sprintf("%s.stderr", commandID)))
	if err != nil {
		return errors.Wrap(err, "creating new process stderr file")
	}
	go func() { _ = filesync(stdout, outPipe) }()
	go func() { _ = filesync(stderr, errPipe) }()
	return nil
}

// Close closes a Group.
func (g *Groups) Close(groupName string) error {
	grp := g.getGroup(groupName)
	if grp == nil {
		return nil
	}
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := g.closeTx(tx, groupName, grp); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "committing transaction")
	}
	return nil
}

// closeTx closes a group ands updates the database using the provided Tx.
func (g *Groups) closeTx(tx *sql.Tx, groupName string, grp *Group) error {
	var (
		cmds    = grp.Commands()
		actions = make([]action, len(cmds))
	)
	for i, cmd := range cmds {
		actions[i] = action{
			key:       actionCmdStop,
			commandID: cmd.ID,
			groupName: groupName,
			processID: cmd.Process.Pid,
		}
	}
	query, args := insertActions(actions...)
	if _, err := tx.Exec(query, args...); err != nil {
		return errors.Wrap(err, "inserting actions")
	}
	if err := grp.Signal(syscall.SIGKILL); err != nil {
		if !strings.HasSuffix(err.Error(), "process already finished") {
			return errors.Wrap(err, "signalling process group")
		}
	}
	// Arbitrary timeout.
	return errors.Wrap(grp.Wait(2*time.Second), "waiting for process group")
}

// Create creates a new group with the provided name.
func (g *Groups) Create(groupName string, cmds ...*Cmd) error {
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := g.createTx(tx, groupName, cmds...); err != nil {
		_ = tx.Rollback()
		return err
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

func (g *Groups) createTx(tx *sql.Tx, groupName string, cmds ...*Cmd) error {
	query, args := insertActions(
		action{
			key:       actionGroupCreate,
			commandID: "",
			groupName: groupName,
		},
	)
	if _, err := tx.Exec(query, args...); err != nil {
		return err
	}
	grp := NewGroup()
	for _, cmd := range cmds {
		if err := g.startTx(tx, cmd, groupName, grp); err != nil {
			return errors.Wrap(err, "starting command")
		}
		if err := insertCmd(tx, groupName, cmd); err != nil {
			return errors.Wrap(err, "inserting new command")
		}
	}
	g.groupsMu.Lock()
	g.groups[groupName] = grp
	g.groupsMu.Unlock()
	return nil
}

const getCommandArgs = `
SELECT		arg
FROM		command_args
WHERE		command_id = ?`

func (g *Groups) getCommandArgs(cid int) ([]string, error) {
	rows, err := g.db.Query(getCommandArgs, cid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() // Best effort.

	args := []string{}
	for rows.Next() {
		var arg string
		if err := rows.Scan(&arg); err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, rows.Err()
}

const getCommandEnv = `
SELECT		env
FROM		command_env
WHERE		command_id = ?`

func (g *Groups) getCommandEnv(cid int) ([]string, error) {
	rows, err := g.db.Query(getCommandEnv, cid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() // Best effort.

	env := []string{}
	for rows.Next() {
		var e string
		if err := rows.Scan(&e); err != nil {
			return nil, err
		}
		env = append(env, e)
	}
	return env, rows.Err()
}

// getGroup gets a named group.
func (g *Groups) getGroup(name string) *Group {
	g.groupsMu.RLock()
	grp := g.groups[name]
	g.groupsMu.RUnlock()
	return grp
}

const getGroupCommands = `
SELECT		cmd.command_id, arg, env_var
FROM		commands cmd
LEFT JOIN	command_args args
ON		cmd.command_id = args.command_id
LEFT JOIN	command_env env
ON		cmd.command_id = env.command_id
WHERE		cmd.group_name = ?`

// getGroupCommandsTx gets the command ID's for a group.
func (g *Groups) getGroupCommandsTx(tx *sql.Tx, groupName string) ([]*Cmd, error) {
	rows, err := tx.Query(getGroupCommands, groupName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() // Best effort.

	commandsMap := map[string]*Cmd{}

	for rows.Next() {
		var (
			commandID string
			arg       = sql.NullString{}
			envvar    = sql.NullString{}
		)
		if err := rows.Scan(&commandID, &arg, &envvar); err != nil {
			return nil, err
		}
		if _, ok := commandsMap[commandID]; !ok {
			commandsMap[commandID] = &Cmd{Cmd: &exec.Cmd{}, ID: commandID}
		}
		if arg.Valid {
			commandsMap[commandID].Args = append(commandsMap[commandID].Args, arg.String)
		}
		if envvar.Valid {
			commandsMap[commandID].Env = append(commandsMap[commandID].Env, envvar.String)
		}

	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "scanning group commands row")
	}
	var (
		commands = make([]*Cmd, len(commandsMap))
		i        = 0
	)
	for id, cmd := range commandsMap {
		commands[i] = &Cmd{
			Cmd: exec.Command(cmd.Args[0], cmd.Args[1:]...),
			ID:  id,
		}
		i++
	}
	return commands, nil
}

func (g *Groups) initialize() error {
	sqldata, err := Asset("sql/createTables.sql")
	if err != nil {
		return errors.Wrap(err, "getting sql data")
	}
	_, err = g.db.Exec(string(sqldata))
	return errors.Wrap(err, "creating tables")
}

// Logs returns a *bufio.Scanner that can be used to
// read the logs of a process in the current group.
// Pass 1 to get stdout and 2 to get stderr.
func (g *Groups) Logs(commandID string, fd int) (*bufio.Scanner, error) {
	var filename string
	switch fd {
	default:
		return nil, errors.Errorf("fd (%d) must be either 1 (stdout) or 2 (stderr)", fd)
	case 1:
		filename = fmt.Sprintf("%s.stdout", commandID)
	case 2:
		filename = fmt.Sprintf("%s.stderr", commandID)
	}
	f, err := os.Open(filepath.Join(g.root, filename))
	if err != nil {
		return nil, err
	}
	return bufio.NewScanner(f), nil
}

// Open opens the Group with the provided name and sets it to the current Group.
// If there is no Group with the provided name then this method initializes a new one.
func (g *Groups) Open(groupName string) ([]*Cmd, error) {
	tx, err := g.db.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "starting transaction")
	}
	cmds, err := g.getGroupCommandsTx(tx, groupName)
	if err != nil {
		return nil, errors.Wrap(err, "getting group commands")
	}
	grp := NewGroup()
	if err := g.openTx(tx, groupName, grp, cmds...); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	g.groupsMu.Lock()
	g.groups[groupName] = grp
	g.groupsMu.Unlock()
	return cmds, errors.Wrap(tx.Commit(), "committing transaction")
}

func (g *Groups) openTx(tx *sql.Tx, groupName string, grp *Group, cmds ...*Cmd) error {
	for _, cmd := range cmds {
		if err := g.startTx(tx, cmd, groupName, grp); err != nil {
			return err
		}
	}
	return nil
}

func (g *Groups) startTx(tx *sql.Tx, cmd *Cmd, groupName string, grp *Group) error {
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "getting stdout pipe")
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "getting stderr pipe")
	}
	if err := g.captureOutput(outPipe, errPipe, cmd.ID); err != nil {
		return errors.Wrap(err, "capturing output of child process")
	}
	if err := grp.Start(cmd); err != nil {
		return errors.Wrap(err, "starting child process")
	}
	query, args := insertActions(
		action{
			key:       actionCmdStart,
			commandID: cmd.ID,
			groupName: groupName,
			processID: cmd.Process.Pid,
		},
	)
	_, err = tx.Exec(query, args...)
	return errors.Wrap(err, "inserting cmd start action")
}

// Wait waits for a process group to finish.
func (g *Groups) Wait(groupName string) error {
	return g.getGroup(groupName).Wait(10 * time.Second)
}

const insertCmdQuery = `INSERT INTO commands (command_id, group_name) VALUES (?, ?)`

// insertCmd inserts a command in the database along with its args and environment variables.
// Calling code is expected to roll back the transaction if this func returns an error.
func insertCmd(tx *sql.Tx, groupName string, cmd *Cmd) error {
	if _, err := tx.Exec(insertCmdQuery, cmd.ID, groupName); err != nil {
		return errors.Wrap(err, "inserting command")
	}
	if len(cmd.Args) > 0 {
		if err := insertCmdArgs(tx, cmd.ID, cmd.Args); err != nil {
			return err
		}
	}
	if len(cmd.Env) > 0 {
		if err := insertCmdEnv(tx, cmd.ID, cmd.Env); err != nil {
			return err
		}
	}
	return nil
}

func insertCmdArgs(tx *sql.Tx, cid string, args []string) error {
	var (
		insertCmdArgsQuery = `INSERT INTO command_args (command_id, idx, arg) VALUES`
		argsArgs           = make([]interface{}, 3*len(args))
	)
	for i, arg := range args {
		if i == 0 {
			insertCmdArgsQuery += ` (?, ?, ?)`
		} else {
			insertCmdArgsQuery += `, (?, ?, ?)`
		}
		argsArgs[(i*3)+0] = cid
		argsArgs[(i*3)+1] = i
		argsArgs[(i*3)+2] = arg
	}
	_, err := tx.Exec(insertCmdArgsQuery, argsArgs...)
	return errors.Wrap(err, "inserting command arguments")
}

func insertCmdEnv(tx *sql.Tx, cid string, env []string) error {
	var (
		insertCmdEnvQuery = `INSERT INTO command_env  (command_id, idx, env) VALUES`
		envArgs           = make([]interface{}, 3*len(env))
	)
	for i, env := range env {
		if i == 0 {
			insertCmdEnvQuery += ` (?, ?, ?)`
		} else {
			insertCmdEnvQuery += `, (?, ?, ?)`
		}
		envArgs[(i*3)+0] = cid
		envArgs[(i*3)+1] = i
		envArgs[(i*3)+2] = env
	}
	_, err := tx.Exec(insertCmdEnvQuery, envArgs...)
	return errors.Wrap(err, "inserting command env")
}

// filesync copies data from an io.Reader to a file.
func filesync(dst *os.File, src io.Reader) error {
	buf := make([]byte, os.Getpagesize())
	for {
		if _, err := src.Read(buf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if _, err := dst.Write(buf); err != nil {
			return err
		}
		if err := dst.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// command is a utility type used to encode/decode commands.
type command struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
	Path string   `json:"path"`
}

// action is a utility type used to insert actions against process groups
// into the groups log.
type action struct {
	key       string
	commandID string
	groupName string
	processID int
}

// Actions
const (
	actionCmdStart    = "command_start"
	actionCmdStop     = "command_stop"
	actionGroupCreate = "group_create"
	actionGroupRemove = "group_remove"
)

func insertActions(actions ...action) (query string, args []interface{}) {
	query = `INSERT INTO groups_log (action_name, command_id, group_name, process_id) VALUES`
	args = make([]interface{}, 4*len(actions))

	for i, action := range actions {
		if i == 0 {
			query += ` (?, ?, ?, ?)`
		} else {
			query += `, (?, ?, ?, ?)`
		}
		args[(i*3)+0] = action.key
		args[(i*3)+1] = action.commandID
		args[(i*3)+2] = action.groupName
		args[(i*3)+3] = action.processID
	}
	return query, args
}
