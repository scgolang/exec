CREATE TABLE IF NOT EXISTS groups_log (
	log_sequence_number	INTEGER		PRIMARY KEY AUTOINCREMENT,
	action_name		TEXT,
	command_id		TEXT,
	group_name		TEXT
);

CREATE INDEX IF NOT EXISTS action_idx ON groups_log (log_sequence_number, action_name);

CREATE TABLE IF NOT EXISTS commands (
	command_id		TEXT,
	pid			INTEGER,
	group_name		TEXT
);

CREATE TABLE IF NOT EXISTS command_args (
	command_id		TEXT,
	idx			INTEGER,
	arg			TEXT
);

CREATE TABLE IF NOT EXISTS command_env (
	command_id		TEXT,
	idx			INTEGER,
	env_var			TEXT
);