package river

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/WangXiangUSTC/go-mysql-mongodb/mongodb"
	"github.com/siddontang/go-mysql/client"
	. "gopkg.in/check.v1"
)

var my_addr = flag.String("my_addr", "127.0.0.1:3306", "MySQL addr")
var mongo_addr = flag.String("mongo_addr", "127.0.0.1:27017", "Elasticsearch addr")

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
	c *client.Conn
	r *River
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	var err error
	s.c, err = client.Connect(*my_addr, "root", "", "test")
	c.Assert(err, IsNil)

	s.testExecute(c, "SET SESSION binlog_format = 'ROW'")

	schema := `	
        CREATE TABLE IF NOT EXISTS %s (	
            id INT,	
            title VARCHAR(256),	
            content VARCHAR(256),	
            mylist VARCHAR(256),	
            tenum ENUM("e1", "e2", "e3"),	
            tset SET("a", "b", "c"),
            PRIMARY KEY(id)) ENGINE=INNODB;	
    `

	s.testExecute(c, "DROP TABLE IF EXISTS test_river")
	s.testExecute(c, fmt.Sprintf(schema, "test_river"))
	s.testExecute(c, fmt.Sprintf(schema, "test_for_id"))

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		s.testExecute(c, fmt.Sprintf(schema, table))
	}

	cfg := new(Config)
	cfg.MyAddr = *my_addr
	cfg.MyUser = "root"
	cfg.MyPassword = ""
	cfg.MyCharset = "utf8"
	cfg.MongoAddr = *mongo_addr

	cfg.ServerID = 1001
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river"
	cfg.DumpExec = "mysqldump"

	cfg.StatAddr = "127.0.0.1:12800"
	cfg.BulkSize = 1
	cfg.FlushBulkTime = TomlDuration{3 * time.Millisecond}

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: "test", Tables: []string{"test_river", "test_river_[0-9]{4}", "test_for_id"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: "test",
			Table:        "test_river",
			Database:     "river",
			Collection:   "river",
			FieldMapping: map[string]string{"title": "mongo_title", "mylist": "mongo_mylist,list"},
		},

		&Rule{Schema: "test",
			Table:        "test_for_id",
			Database:     "river",
			Collection:   "river",
			ID:           []string{"id", "title"},
			FieldMapping: map[string]string{"title": "mongo_title", "mylist": "mongo_mylist,list"},
		},

		&Rule{Schema: "test",
			Table:        "test_river_[0-9]{4}",
			Database:     "river",
			Collection:   "river",
			FieldMapping: map[string]string{"title": "mongo_title", "mylist": "mongo_mylist,list"},
		},
	}

	s.r, err = NewRiver(cfg)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	s.testCleanData(c)

	if s.c != nil {
		s.c.Close()
	}

	if s.r != nil {
		s.r.Close()
	}
}

func (s *riverTestSuite) TestConfig(c *C) {
	str := `	
my_addr = "127.0.0.1:3306"	
my_user = "root"	
my_pass = ""	
my_charset = "utf8"	
mongo_addr = "127.0.0.1:27017"	
data_dir = "./var"	
[[source]]	
schema = "test"	
tables = ["test_river", "test_river_[0-9]{4}", "test_for_id"]
[[rule]]
schema = "test"
table = "test_river"
database = "river"	
collection = "river"
    [rule.field]
    title = "mongo_title"
    mylist = "mongo_mylist,list"
[[rule]]	
schema = "test"	
table = "test_for_id"	
database = "river"	
collection = "river"	
id = ["id", "title"]	
    [rule.field]
    title = "mongo_title"
    mylist = "mongo_mylist,list"
[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
database = "river"
collection = "river"
    [rule.field]	
    title = "mongo_title"	
    mylist = "mongo_mylist,list"	
`

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Sources[0].Tables, HasLen, 3)
	c.Assert(cfg.Rules, HasLen, 3)
}

func (s *riverTestSuite) testExecute(c *C, query string, args ...interface{}) {
	fmt.Println(query, args)
	_, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 2, "second", "hello mysql 2", "e2", "b,c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 3, "third", "hello mongodb 3", "e3", "c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 4, "fouth", "hello go-mysql-mongodb 4", "e1", "a,b,c")
	s.testExecute(c, "INSERT INTO test_for_id (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("INSERT INTO %s (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", table), 5+i, "abc", "hello", "e1", "a,b,c")
	}
}

func (s *riverTestSuite) testCleanData(c *C) {
	for i := 1; i <= 4; i++ {
		s.testExecute(c, "DELETE FROM test_river WHERE id = ?", i)
	}
	s.testExecute(c, "DELETE FROM test_for_id WHERE id = ?", 1)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("DELETE FROM %s WHERE id = ?", table), 5+i)
	}
}

func (s *riverTestSuite) testMongoGet(c *C, id string) *mongodb.Response {
	database := "river"
	collection := "river"

	r, err := s.r.mongo.Get(database, collection, id)
	c.Assert(err, IsNil)

	return r
}

func testWaitSyncDone(c *C, r *River) {
	<-r.canal.WaitDumpDone()

	err := r.canal.CatchMasterPos(10 * time.Second)
	c.Assert(err, IsNil)

	for i := 0; i < 1000; i++ {
		if len(r.syncCh) == 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	c.Fatalf("wait 1s but still have %d items to be synced", len(r.syncCh))
}

func (s *riverTestSuite) TestRiver(c *C) {
	s.testPrepareData(c)

	s.r.Start()

	testWaitSyncDone(c, s.r)

	var r *mongodb.Response
	r = s.testMongoGet(c, "1")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["tenum"], Equals, "e1")
	c.Assert(r.Source["tset"], Equals, "a,b")

	r = s.testMongoGet(c, "1:first")
	c.Assert(r.Found, Equals, true)

	r = s.testMongoGet(c, "100")
	c.Assert(r.Found, Equals, false)

	for i := 0; i < 10; i++ {
		r = s.testMongoGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["mongo_title"], Equals, "abc")
	}

	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	s.testExecute(c, "DELETE FROM test_river WHERE id = ?", 1)
	s.testExecute(c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	// so we can insert invalid data
	s.testExecute(c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	testWaitSyncDone(c, s.r)

	r = s.testMongoGet(c, "1")
	c.Assert(r.Found, Equals, false)

	r = s.testMongoGet(c, "2")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["mongo_title"], Equals, "second 2")
	c.Assert(r.Source["tenum"], Equals, "e3")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["mongo_mylist"], DeepEquals, []interface{}{"a", "b", "c"})

	r = s.testMongoGet(c, "4")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["tenum"], Equals, "")
	c.Assert(r.Source["tset"], Equals, "a,b,c")

	r = s.testMongoGet(c, "3")
	c.Assert(r.Found, Equals, false)

	r = s.testMongoGet(c, "30")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["mongo_title"], Equals, "second 30")

	for i := 0; i < 10; i++ {
		r = s.testMongoGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["mongo_title"], Equals, "hello")
	}
}
