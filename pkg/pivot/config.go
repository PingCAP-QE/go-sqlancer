package pivot

import (
	"errors"
	"regexp"
	"strings"
)

var (
	dsnPattern = regexp.MustCompile(`^(\w*?)\:?(\w*?)\@tcp\((\S*?)\:(\d*?)\)\/(.*?)$`)
)

// Config struct
type Config struct {
	DSN    string
	DBName string
	Depth  int
	Silent bool
	Debug  bool

	ViewCount int
	Hint      bool
	ExprIndex bool
	// TODO implement them
	PrepareStmt bool
}

// NewConfig create default config
func NewConfig() *Config {
	return &Config{
		DSN:         "",
		PrepareStmt: false,
		Hint:        false,
		Depth:       1,
		Silent:      false,
		Debug:       false,
		ViewCount:   10,
		ExprIndex:   false,
	}
}

// SetDSN set dsn and parse dbname
func (conf *Config) SetDSN(dsn string) error {
	dsn = strings.Trim(dsn, " ")

	dsnMatches := dsnPattern.FindStringSubmatch(dsn)
	if len(dsnMatches) == 6 {
		if dsnMatches[5] == "" {
			conf.DBName = "test"
			conf.DSN = dsn
		} else {
			conf.DBName = dsnMatches[5]
			conf.DSN = strings.TrimRight(dsn, dsnMatches[5])
		}
	} else {
		return errors.New("invalid dsn")
	}
	return nil
}
