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

	// curName is the name of the current process group.
	curName string

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
		root: absRoot,
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
func (g *Groups) captureOutput(outPipe, errPipe io.ReadCloser, pid int) error {
	stdout, err := os.Create(filepath.Join(g.root, fmt.Sprintf("%d.stdout", pid)))
	if err != nil {
		return errors.Wrap(err, "creating new process stdout file")
	}
	stderr, err := os.Create(filepath.Join(g.root, fmt.Sprintf("%d.stderr", pid)))
	if err != nil {
		return errors.Wrap(err, "creating new process stderr file")
	}
	go func() { _, _ = io.Copy(stdout, outPipe) }()
	go func() { _, _ = io.Copy(stderr, errPipe) }()
	return nil
}

// CloseCurrent closes the current Group.
func (g *Groups) CloseCurrent() error {
	if g.cur == nil {
		return nil
	}
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := g.closeTx(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "committing transaction")
	}
	g.cur = nil
	return nil
}

// closeTx closes the current group and logs info about the
// closed commands using the provided *sql.Tx
func (g *Groups) closeTx(tx *sql.Tx) error {
	var (
		cmds    = g.cur.Commands()
		actions = make([]action, len(cmds))
	)
	for i, cmd := range cmds {
		actions[i] = action{
			key:       actionCmdStop,
			commandID: cmd.ID,
			groupName: g.curName,
		}
	}
	query, args := insertActions(actions...)
	if _, err := tx.Exec(query, args...); err != nil {
		return errors.Wrap(err, "inserting actions")
	}
	if err := g.cur.Signal(syscall.SIGKILL); err != nil {
		if !strings.HasSuffix(err.Error(), "process already finished") {
			return errors.Wrap(err, "signalling current process group")
		}
	}
	if err := g.cur.Wait(2 * time.Second); err != nil {
		return errors.Wrap(err, "waiting for process group")
	}
	return nil
}

// Create creates a new group with the provided name.
func (g *Groups) Create(groupName string, cmds ...*Cmd) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	// TODO: kill current group if there is one
	query, args := insertActions(
		action{actionGroupCreate, "", groupName},
		action{actionSetCurrent, "", groupName},
	)
	if _, err := tx.Exec(query, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	g.cur = NewGroup()
	g.curName = groupName

	for _, cmd := range cmds {
		if err := g.startTx(tx, cmd); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

const getCurrent = `
SELECT		group_name
FROM		groups_log
WHERE		action = '` + actionSetCurrent + `'
ORDER BY	log_sequence_number DESC
LIMIT		1`

// GetCurrent gets the current process group.
func (g *Groups) GetCurrent() (int, error) {
	var gid int
	err := g.db.QueryRow(getCurrent).Scan(&gid)
	return gid, err
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

const getGroupCommands = `
SELECT		cmd.command_id, arg, env_var
FROM		commands cmd
LEFT JOIN	command_args args
ON		cmd.command_id = args.command_id
LEFT JOIN	command_env env
ON		cmd.command_id = env.command_id
WHERE		cmd.group_name = ?`

// getGroupCommands gets the command ID's for a group.
func (g *Groups) getGroupCommands(groupName string) ([]*Cmd, error) {
	rows, err := g.db.Query(getGroupCommands, groupName)
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
			commandsMap[commandID] = &Cmd{}
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
	if _, err := g.db.Exec(string(sqldata)); err != nil {
		return errors.Wrap(err, "creating tables")
	}
	return errors.Wrap(g.startCurrent(), "starting the current group")
}

const getPid = `SELECT pid FROM commands WHERE command_id = ? LIMIT 1`

// Logs returns a *bufio.Scanner that can be used to
// read the logs of a process in the current group.
// Pass 1 to get stdout and 2 to get stderr.
func (g *Groups) Logs(commandID string, fd int) (*bufio.Scanner, error) {
	var (
		filename string
		pid      int
	)
	if err := g.db.QueryRow(getPid, commandID).Scan(&pid); err != nil {
		return nil, errors.Wrap(err, "querying pid for command "+commandID)
	}
	switch fd {
	default:
		return nil, errors.Errorf("fd (%d) must be either 1 (stdout) or 2 (stderr)", fd)
	case 1:
		filename = fmt.Sprintf("%d.stdout", pid)
	case 2:
		filename = fmt.Sprintf("%d.stderr", pid)
	}
	f, err := os.Open(filepath.Join(g.root, filename))
	if err != nil {
		return nil, err
	}
	return bufio.NewScanner(f), nil
}

// Open opens the Group with the provided name and sets it to the current Group.
// If there is no Group with the provided name then this method initializes a new one.
func (g *Groups) Open(name string) error {
	return nil
}

// Start starts a process in the current group.
func (g *Groups) Start(cmd *Cmd) error {
	// Early out if there are no groups yet.
	if g.cur == nil {
		return errors.New("no current group")
	}
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := g.startTx(tx, cmd); err != nil {
		_ = tx.Rollback()
		return err
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

// startCurrent ensures that the current group in the database is the currently active group.
func (g *Groups) startCurrent() error {
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := g.startCurrentTx(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

const getCurrentGroup = `
SELECT		group_name
FROM		groups_log
WHERE		action_name = '` + actionSetCurrent + `'
ORDER BY	log_sequence_number ASC
LIMIT		1`

// startCurrentTx starts the current group in the database using the provided *sql.Tx
func (g *Groups) startCurrentTx(tx *sql.Tx) error {
	var groupName string
	if err := tx.QueryRow(getCurrentGroup).Scan(&groupName); err != nil {
		if err != sql.ErrNoRows {
			return errors.Wrap(err, "getting current group name")
		}
	}
	if groupName != "" && groupName == g.curName {
		return nil
	}
	if err := g.CloseCurrent(); err != nil {
		return errors.Wrap(err, "closing current group")
	}
	cmds, err := g.getGroupCommands(groupName)
	if err != nil {
		return errors.Wrap(err, "getting group commands")
	}
	for _, cmd := range cmds {
		if err := g.startTx(tx, cmd); err != nil {
			return err
		}
	}
	return nil
}

func (g *Groups) startTx(tx *sql.Tx, cmd *Cmd) error {
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "getting stdout pipe")
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "getting stderr pipe")
	}
	if err := g.cur.Start(cmd); err != nil {
		return errors.Wrap(err, "starting child process")
	}
	if err := insertCmd(tx, g.curName, cmd); err != nil {
		return errors.Wrap(err, "inserting new command")
	}
	if err := g.captureOutput(outPipe, errPipe, cmd.Process.Pid); err != nil {
		return errors.Wrap(err, "capturing output of child process")
	}
	query, args := insertActions(
		action{actionCmdStart, cmd.ID, g.curName},
	)
	_, err = tx.Exec(query, args...)
	return errors.Wrap(err, "inserting cmd start action")
}

const insertCmdQuery = `INSERT INTO commands (command_id, pid, group_name) VALUES (?, ?, ?)`

// insertCmd inserts a command in the database along with its args and environment variables.
// Calling code is expected to roll back the transaction if this func returns an error.
func insertCmd(tx *sql.Tx, groupName string, cmd *Cmd) error {
	if _, err := tx.Exec(insertCmdQuery, cmd.ID, cmd.Process.Pid, groupName); err != nil {
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
}

// Actions
const (
	actionCmdStart    = "command_start"
	actionCmdStop     = "command_stop"
	actionGroupCreate = "group_create"
	actionGroupRemove = "group_remove"
	actionSetCurrent  = "set_current"
)

func insertActions(actions ...action) (query string, args []interface{}) {
	query = `INSERT INTO groups_log (action_name, command_id, group_name) VALUES`
	args = make([]interface{}, 3*len(actions))

	for i, action := range actions {
		if i == 0 {
			query += ` (?, ?, ?)`
		} else {
			query += `, (?, ?, ?)`
		}
		args[(i*3)+0] = action.key
		args[(i*3)+1] = action.commandID
		args[(i*3)+2] = action.groupName
	}
	return query, args
}
