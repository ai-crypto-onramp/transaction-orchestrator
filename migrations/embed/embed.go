package migrations

import _ "embed"

//go:embed 0001_init.up.sql
var InitUp string

//go:embed 0001_init.down.sql
var InitDown string