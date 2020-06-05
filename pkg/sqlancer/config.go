package sqlancer

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
	DSN      string
	DBName   string
	Depth    int
	Silent   bool
	LogLevel string

	TotalViewCount int

	EnableHint      bool
	EnableExprIndex bool
	EnableNoRECMode bool
	EnablePQSMode   bool
}

// NewConfig create default config
func NewConfig() *Config {
	return &Config{
		DSN:             "",
		Depth:           1,
		Silent:          false,
		LogLevel:        "info",
		TotalViewCount:  10,
		EnableHint:      false,
		EnableExprIndex: false,
		EnableNoRECMode: false,
		EnablePQSMode:   false,
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
