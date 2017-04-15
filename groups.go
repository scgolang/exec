package exec

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

const (
	// DataDir is the directory that contains the internal state of Groups
	DataDir = ".data"

	// GroupsDB is the name of the sqlite database file.
	GroupsDB = "groups.db"
)

const dirPerms = 0755

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
func NewGroups(root string) (*Groups, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	g := &Groups{
		root: absRoot,
	}
	dataPath := filepath.Join(g.root, DataDir)
	info, err := os.Stat(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(dataPath, dirPerms); err != nil {
				return nil, errors.Wrap(err, "creating "+dataPath+" directory")
			}
		}
	}
	if info != nil && !info.IsDir() {
		return nil, errors.Wrap(err, dataPath+" is not a directory")
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, DataDir, GroupsDB))
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}
	g.db = db
	if err := g.initialize(); err != nil {
		return nil, errors.Wrap(err, "initializing groups")
	}
	return g, nil
}

// Close closes the current Group.
func (g *Groups) Close() error {
	return nil
}

// Create creates a new group with the provided name.
func (g *Groups) Create(groupName string) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	// TODO: remove current group if there is one
	query, args := insertActions([]action{
		{actionGroupCreate, "", groupName},
		{actionSetCurrent, "", groupName},
	}...)
	if _, err := tx.Exec(query, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	g.cur = NewGroup()
	g.curName = groupName
	return nil
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
SELECT		command_id, path, arg, env
FROM		commands cmd
LEFT JOIN	command_args args
ON		cmd.command_id = args.command_id
LEFT JOIN	command_env env
ON		cmd.command_id = groups.group_id
WHERE		cmd.group_name = ?`

// getGroupCommands gets the command ID's for a group.
func (g *Groups) getGroupCommands(groupName string) (map[int]*command, error) {
	rows, err := g.db.Query(getGroupCommands, groupName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() // Best effort.

	commands := map[int]*command{}

	for rows.Next() {
		var (
			cid    int
			path   string
			arg    = sql.NullString{}
			envvar = sql.NullString{}
		)
		if err := rows.Scan(&cid, &arg, &envvar); err != nil {
			return nil, err
		}
		if _, ok := commands[cid]; !ok {
			commands[cid] = &command{Path: path}
		}
		if arg.Valid {
			commands[cid].Args = append(commands[cid].Args, arg.String)
		}
		if envvar.Valid {
			commands[cid].Env = append(commands[cid].Env, envvar.String)
		}

	}
	return commands, rows.Err()
}

const createLogTable = `
CREATE TABLE IF NOT EXISTS groups_log (
	log_sequence_number	INTEGER		PRIMARY KEY AUTOINCREMENT,
	action			TEXT,
	command_id		TEXT,
	group_name		TEXT
)`

const createActionsIndex = `
CREATE INDEX IF NOT EXISTS action_idx ON groups_log (log_sequence_number, action)`

const createCommandsTable = `
CREATE TABLE IF NOT EXISTS commands (
	command_id		TEXT,
	group_name		TEXT
)`

const createCommandArgsTable = `
CREATE TABLE IF NOT EXISTS command_args (
	command_id		TEXT,
	idx			INTEGER,
	arg			TEXT
)`

const createCommandEnvTable = `
CREATE TABLE IF NOT EXISTS command_env (
	command_id		TEXT,
	idx			INTEGER,
	env_var			TEXT
)`

func (g *Groups) initialize() error {
	for _, s := range []struct {
		errmsg string
		sql    string
	}{
		{errmsg: "creating log table", sql: createLogTable},
		{errmsg: "creating actions index", sql: createActionsIndex},
		{errmsg: "creating commands table", sql: createCommandsTable},
		{errmsg: "creating command args table", sql: createCommandArgsTable},
		{errmsg: "creating command env table", sql: createCommandEnvTable},
	} {
		if _, err := g.db.Exec(s.sql); err != nil {
			return errors.Wrap(err, s.errmsg)
		}
	}
	return nil
}

// Open opens the Group with the provided name and sets it to the current Group.
// If there is no Group with the provided name then this method initializes a new one.
func (g *Groups) Open(name string) error {
	return nil
}

// startGroup starts all the processes for the named group and sets it to the current group.
func (g *Groups) startGroup(groupName string) error {
	// // TODO: get rid of n+1
	// cids, err := g.getGroupCommands(groupName)
	// if err != nil {
	// 	return errors.Wrap(err, "getting group commands")
	// }
	// for _, cid := range cids {
	// }
	// // Decode the existing process file and start a new process.
	// procFile, err := os.Create(filepath.Join(existingProcPath, processFile))
	// if err != nil {
	// 	return errors.Wrap(err, "creating new process file")
	// }
	// cmdenc := command{}
	// if err := json.NewDecoder(procFile).Decode(&cmdenc); err != nil {
	// 	return errors.Wrap(err, "decoding "+processFile)
	// }
	// // Args should always have the command name at index 0
	// cmd := exec.Command(cmdenc.Path, cmdenc.Args[1:]...)

	// // TODO: will we need to modify the env of the new process?
	// cmd.Env = cmdenc.Env

	// stdoutPipe, err := cmd.StdoutPipe()
	// if err != nil {
	// 	return errors.Wrap(err, "getting stdout pipe")
	// }
	// stderrPipe, err := cmd.StderrPipe()
	// if err != nil {
	// 	return errors.Wrap(err, "getting stderr pipe")
	// }
	// if err := group.Start(cmd); err != nil {
	// 	return err
	// }
	// procPath := filepath.Join(gpath, strconv.Itoa(cmd.Process.Pid))
	// if err := os.Mkdir(procPath, dirPerms); err != nil {
	// 	return errors.Wrap(err, "making new process directory")
	// }
	// // Pipe stdout and stderr.
	// stdout, err := os.Create(filepath.Join(procPath, stdoutFile))
	// if err != nil {
	// 	return errors.Wrap(err, "creating new process stdout file")
	// }
	// stderr, err := os.Create(filepath.Join(procPath, stderrFile))
	// if err != nil {
	// 	return errors.Wrap(err, "creating new process stderr file")
	// }
	// go func() { _, _ = io.Copy(stdout, stdoutPipe) }()
	// go func() { _, _ = io.Copy(stderr, stderrPipe) }()
	return nil
}

// Start starts a process in the current group.
func (g *Groups) Start(commandID string, cmd *exec.Cmd) error {
	if g.cur == nil {
		return errors.New("no current group")
	}
	tx, err := g.db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	if err := insertCmd(tx, g.curName, commandID, cmd); err != nil {
		_ = tx.Rollback()
		return errors.Wrap(err, "inserting new command")
	}
	if err := g.cur.Start(cmd); err != nil {
		_ = tx.Rollback()
		return errors.Wrap(err, "starting child process")
	}
	query, args := insertActions([]action{
		{actionCmdStart, commandID, g.curName},
	}...)
	if _, err := tx.Exec(query, args...); err != nil {
		_ = tx.Rollback()
		return errors.Wrap(err, "inserting cmd start action")
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

const insertCmdQuery = `INSERT INTO commands (command_id, group_name) VALUES (?, ?)`

// insertCmd inserts a command in the database along with its args and environment variables.
// Calling code is expected to roll back the transaction if this func returns an error.
func insertCmd(tx *sql.Tx, groupName, commandID string, cmd *exec.Cmd) error {
	if _, err := tx.Exec(insertCmdQuery, commandID, groupName); err != nil {
		return errors.Wrap(err, "inserting command")
	}
	if len(cmd.Args) > 0 {
		if err := insertCmdArgs(tx, commandID, cmd.Args); err != nil {
			return err
		}
	}
	if len(cmd.Env) > 0 {
		if err := insertCmdEnv(tx, commandID, cmd.Env); err != nil {
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
	query = `INSERT INTO groups_log (action, group_name) VALUES`
	args = make([]interface{}, 2*len(actions))

	for i, action := range actions {
		if i == 0 {
			query += ` (?, ?)`
		} else {
			query += `, (?, ?)`
		}
		args[(i*2)+0] = action.key
		args[(i*2)+1] = action.groupName
	}
	return query, args
}
