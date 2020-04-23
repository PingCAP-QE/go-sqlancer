package pivot

type Config struct {
	Dsn string

	// TODO implement them
	PrepareStmt bool
	Hint        bool
}

// default config
func NewConfig() *Config {
	return &Config{}
}
