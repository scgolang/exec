CREATE TABLE IF NOT EXISTS commands (
	command_id		TEXT,
	group_name		TEXT,

	UNIQUE (command_id, group_name)
);

CREATE INDEX IF NOT EXISTS group_name_idx ON commands (command_id, group_name);

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

CREATE TABLE IF NOT EXISTS processes (
	command_id		TEXT,
	process_id		INTEGER
);
