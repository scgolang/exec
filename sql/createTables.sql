CREATE TABLE IF NOT EXISTS commands (
	command_id		TEXT		PRIMARY KEY,
	group_name		TEXT
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
