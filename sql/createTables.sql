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
	group_name		TEXT,
	process_id		INTEGER
);
