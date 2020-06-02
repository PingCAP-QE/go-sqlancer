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
	DSN    string
	DBName string
	Depth  int
	Silent bool
	Debug  bool

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
		EnableHint:      false,
		Depth:           1,
		Silent:          false,
		Debug:           false,
		TotalViewCount:  10,
		EnableExprIndex: false,
		EnableNoRECMode: true,
		EnablePQSMode:   true,
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
