package river

import (
	"github.com/siddontang/go-mysql/schema"
)

// If you want to sync MySQL data into MongoDB, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> database + collection.
// schema and table is for MySQL, database and collection type is for MongoDB.
type Rule struct {
	Schema     string   `toml:"schema"`
	Table      string   `toml:"table"`
	Database   string   `toml:"database"`
	Collection string   `toml:"collection"`
	ID         []string `toml:"id"`

	// Default, a MySQL table field name is mapped to MongoDB field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table

	//only MySQL fields in fileter will be synced , default sync all fields
	Fileter []string `toml:"filter"`
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table
	r.Database = schema
	r.Collection = table
	r.FieldMapping = make(map[string]string)

	return r
}

func (r *Rule) prepare() error {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if len(r.Database) == 0 {
		r.Database = r.Table
	}

	if len(r.Collection) == 0 {
		r.Collection = r.Database
	}

	return nil
}

func (r *Rule) CheckFilter(field string) bool {
	if r.Fileter == nil {
		return true
	}

	for _, f := range r.Fileter {
		if f == field {
			return true
		}
	}
	return false
}
