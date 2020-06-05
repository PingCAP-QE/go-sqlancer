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
	LogLevel string
	Silent   bool

	Depth           int
	ViewCount       int
	EnableHint      bool
	EnableExprIndex bool

	EnablePQSApproach   bool
	EnableNoRECApproach bool
	EnableTLPApproach   bool
}

// NewConfig create default config
func NewConfig() *Config {
	return &Config{
		DSN:                 "",
		DBName:              "test",
		Silent:              false,
		Depth:               1,
		LogLevel:            "info",
		ViewCount:           10,
		EnableHint:          false,
		EnableExprIndex:     false,
		EnablePQSApproach:   false,
		EnableNoRECApproach: false,
		EnableTLPApproach:   false,
	}
}

// SetDSN set dsn and parse dbname
func (conf *Config) SetDSN(dsn string) error {
	dsn = strings.Trim(dsn, " ")

	dsnMatches := dsnPattern.FindStringSubmatch(dsn)
	if len(dsnMatches) == 6 {
		if dsnMatches[5] == "" {
			conf.DSN = dsn
			conf.DBName = "test"
		} else {
			conf.DSN = strings.TrimRight(dsn, dsnMatches[5])
			conf.DBName = dsnMatches[5]
		}
	} else {
		return errors.New("invalid dsn")
	}
	return nil
}
