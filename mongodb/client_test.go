package mongodb

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var host = flag.String("host", "127.0.0.1", "MongoDB host")
var port = flag.Int("port", 27017, "MongoDB port")

func Test(t *testing.T) {
	TestingT(t)
}

type mongoTestSuite struct {
	c *Client
}

var _ = Suite(&mongoTestSuite{})

func (s *mongoTestSuite) SetUpSuite(c *C) {
	cfg := new(ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.Username = ""
	cfg.Password = ""
	var err error
	s.c, err = NewClient(cfg)
	c.Assert(err, IsNil)
}

func (s *mongoTestSuite) TearDownSuite(c *C) {

}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func (s *mongoTestSuite) TestSimple(c *C) {
	database := "dummy"
	collection := "blog"

	//key1 := "name"
	//key2 := "content"

	err := s.c.Update(database, collection, "1", makeTestData("abc", "hello world"))
	c.Assert(err, IsNil)

	exists, err := s.c.Exists(database, collection, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)

	r, err := s.c.Get(database, collection, "1")
	c.Assert(err, IsNil)
	c.Assert(r.Code, Equals, 200)
	c.Assert(r.ID, Equals, "1")

	err = s.c.Delete(database, collection, "1")
	c.Assert(err, IsNil)

	exists, err = s.c.Exists(database, collection, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, false)

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.Database = database
		req.Collection = collection
		req.ID = id
		items[i] = req
	}

	err = s.c.Bulk(items)
	c.Assert(err, IsNil)
}
