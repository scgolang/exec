package exec

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3" // Load sqlite driver.
	"github.com/pkg/errors"
	scgolangsql "github.com/scgolang/exec/sql"
)

// DirPerms are permissions for directories created by this package.
const DirPerms = 0755

// CmdError is an error with a particular process.
type CmdError struct {
	Cmd *exec.Cmd
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
func (g *Groups) captureOutput(outPipe, errPipe io.ReadCloser, groupName string, cmd *exec.Cmd) error {
	commandID, err := GetCmdID(cmd)
	if err != nil {
		return errors.Wrap(err, "getting command ID")
	}
	stdout, err := os.Create(filepath.Join(g.root, groupName, fmt.Sprintf("%s.stdout", commandID)))
	if err != nil {
		return errors.Wrap(err, "creating new process stdout file")
	}
	stderr, err := os.Create(filepath.Join(g.root, groupName, fmt.Sprintf("%s.stderr", commandID)))
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
	if err := grp.Signal(syscall.SIGKILL); err != nil {
		if !isAlreadyFinished(err) {
			return errors.Wrap(err, "signalling process group")
		}
	}
	// Arbitrary timeout.
	return errors.Wrap(grp.Wait(2*time.Second), "waiting for process group")
}

// Commands returns the commands that are part of the specified group.
// If a group with the provided name does not exist it returns nil and false,
// otherwise it returns a slice and true.
func (g *Groups) Commands(groupName string) ([]*exec.Cmd, bool) {
	g.groupsMu.RLock()
	grp, ok := g.groups[groupName]
	g.groupsMu.RUnlock()
	if !ok {
		return nil, false
	}
	return grp.Commands(), true
}

// Create creates a new group with the provided name.
func (g *Groups) Create(groupName string, cmds ...*exec.Cmd) error {
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

// createTx creates a group with a sql transaction.
func (g *Groups) createTx(tx *sql.Tx, groupName string, cmds ...*exec.Cmd) error {
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

const getGroupProcesses = `
SELECT		p.command_id, arg, env_var
FROM		processes p
LEFT JOIN	command_args a
ON		p.command_id = a.command_id
LEFT JOIN	command_env e
ON		p.command_id = e.command_id
WHERE		p.group_name = ?`

// getGroupProcessesTx gets the processes for a group from a database using
// the provided sql transaction.
func (g *Groups) getGroupProcessesTx(tx *sql.Tx, groupName string) ([]*exec.Cmd, error) {
	rows, err := tx.Query(getGroupProcesses, groupName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() // Best effort.

	commandsMap := map[string]*exec.Cmd{}

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
			commandsMap[commandID] = &exec.Cmd{}
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
		commands = make([]*exec.Cmd, len(commandsMap))
		i        = 0
	)
	for _, cmd := range commandsMap {
		commands[i] = exec.Command(cmd.Args[0], cmd.Args[1:]...)
		i++
	}
	return commands, nil
}

func (g *Groups) initialize() error {
	sqldata, err := scgolangsql.Asset("createTables.sql")
	if err != nil {
		return errors.Wrap(err, "getting sql data")
	}
	_, err = g.db.Exec(string(sqldata))
	return errors.Wrap(err, "creating tables")
}

// Logs returns a *bufio.Scanner that can be used to
// read the logs of a process in the current group.
// Pass 1 to get stdout and 2 to get stderr.
// Calling code is expected to close the io.Closer that is returned.
func (g *Groups) Logs(groupName string, cmd *exec.Cmd, fd int) (*bufio.Scanner, io.Closer, error) {
	commandID, err := GetCmdID(cmd)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting command ID")
	}
	var filename string
	switch fd {
	default:
		return nil, nil, errors.Errorf("fd (%d) must be either 1 (stdout) or 2 (stderr)", fd)
	case 1:
		filename = fmt.Sprintf("%s.stdout", commandID)
	case 2:
		filename = fmt.Sprintf("%s.stderr", commandID)
	}
	f, err := os.Open(filepath.Join(g.root, groupName, filename))
	if err != nil {
		return nil, nil, err
	}
	return bufio.NewScanner(f), f, nil
}

// Open opens the Group with the provided name and sets it to the current Group.
// If there is no Group with the provided name then this method initializes a new one.
func (g *Groups) Open(groupName string) ([]*exec.Cmd, error) {
	tx, err := g.db.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "starting transaction")
	}
	cmds, err := g.getGroupProcessesTx(tx, groupName)
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

// openTx starts up a process group.
func (g *Groups) openTx(tx *sql.Tx, groupName string, grp *Group, cmds ...*exec.Cmd) error {
	for _, cmd := range cmds {
		if err := g.startTx(tx, cmd, groupName, grp); err != nil {
			return err
		}
	}
	return nil
}

// Remove removes commands from a group, or removes a group entirely
// if there are no command ID's passed.
func (g *Groups) Remove(groupName string, cmds ...*exec.Cmd) error {
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := g.removeTx(tx, groupName, cmds...); err != nil {
		_ = tx.Rollback()
		return err
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

func (g *Groups) removeTx(tx *sql.Tx, groupName string, cmds ...*exec.Cmd) error {
	var (
		args  = make([]interface{}, 1+len(cmds))
		query = `DELETE FROM processes WHERE group_name = ? AND (`
	)
	args[0] = groupName

	for i, cmd := range cmds {
		if i > 0 {
			query += ` OR `
		}
		query += `process_id = ?`
		args[i+1] = cmd.Process.Pid
	}
	query += `)`

	if len(cmds) == 0 {
		query = `DELETE FROM processes WHERE group_name = ?`
	}
	if _, err := g.db.Exec(query, args...); err != nil {
		return errors.Wrap(err, "deleting group commands from database")
	}
	grp := g.getGroup(groupName)

	if grp == nil {
		return errors.Errorf("group %s not found", groupName)
	}
	return errors.Wrap(grp.Remove(cmds...), "removing commands from group")
}

func (g *Groups) startTx(tx *sql.Tx, cmd *exec.Cmd, groupName string, grp *Group) error {
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "getting stdout pipe")
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "getting stderr pipe")
	}
	if err := os.Mkdir(filepath.Join(g.root, groupName), DirPerms); err != nil {
		if !os.IsExist(err) {
			return errors.Wrap(err, "creating group directory")
		}
	}
	if err := g.captureOutput(outPipe, errPipe, groupName, cmd); err != nil {
		return errors.Wrap(err, "capturing output of child process")
	}
	if err := grp.Start(cmd); err != nil {
		return errors.Wrap(err, "starting child process")
	}
	return errors.Wrap(err, "inserting cmd start action")
}

// Wait waits for a process group to finish.
func (g *Groups) Wait(groupName string) error {
	return g.getGroup(groupName).Wait(10 * time.Second)
}

const insertCmdQuery = `INSERT INTO processes (command_id, group_name, process_id)
                        VALUES                (?,          ?,          ?)`

// insertCmd inserts a command in the database along with its args and environment variables.
// Calling code is expected to roll back the transaction if this func returns an error.
func insertCmd(tx *sql.Tx, groupName string, cmd *exec.Cmd) error {
	commandID, err := GetCmdID(cmd)
	if err != nil {
		return errors.Wrap(err, "getting command ID")
	}
	if _, err := tx.Exec(insertCmdQuery, commandID, groupName, cmd.Process.Pid); err != nil {
		return errors.Wrap(err, "inserting command")
	}
	if len(cmd.Args) > 0 {
		if err := insertCmdArgs(tx, commandID, cmd.Args); err != nil {
			return errors.Wrap(err, "inserting command args")
		}
	}
	if len(cmd.Env) > 0 {
		if err := insertCmdEnv(tx, commandID, cmd.Env); err != nil {
			return errors.Wrap(err, "inserting command environment")
		}
	}
	return nil
}

func insertCmdArgs(tx *sql.Tx, commandID string, args []string) error {
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
		argsArgs[(i*3)+0] = commandID
		argsArgs[(i*3)+1] = i
		argsArgs[(i*3)+2] = arg
	}
	_, err := tx.Exec(insertCmdArgsQuery, argsArgs...)
	return errors.Wrap(err, "inserting command arguments")
}

func insertCmdEnv(tx *sql.Tx, commandID string, env []string) error {
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
		envArgs[(i*3)+0] = commandID
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

// GetCmdID hashes the args and env of a command to form a unique ID.
func GetCmdID(cmd *exec.Cmd) (string, error) {
	var (
		h    = sha256.New()
		args = bytes.Join(s2b(cmd.Args), []byte{' '})
		env  = bytes.Join(s2b(cmd.Env), []byte{' '})
	)
	_, err := h.Write(bytes.Join([][]byte{args, env}, []byte{' '}))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func s2b(ss []string) [][]byte {
	bs := make([][]byte, len(ss))
	for i, s := range ss {
		bs[i] = []byte(s)
	}
	return bs
}
