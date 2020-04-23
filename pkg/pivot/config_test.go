package pivot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDSN(t *testing.T) {
	conf := NewConfig()

	assert.Nil(t, conf.SetDSN("root:@tcp(127.0.0.1:4000)/"), "parse dsn no error")
	assert.Equal(t, conf.DSN, "root:@tcp(127.0.0.1:4000)/", "parse dsn")
	assert.Equal(t, conf.DBName, "test", "parse dbname")

	assert.Nil(t, conf.SetDSN("root:passwd@tcp(127.0.0.1:4000)/bank"), "parse dsn no error")
	assert.Equal(t, conf.DSN, "root:passwd@tcp(127.0.0.1:4000)/", "parse dsn")
	assert.Equal(t, conf.DBName, "bank", "parse dbname")
}
