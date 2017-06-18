package mongodb

import (
	"flag"
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
)

var host = flag.String("host", "127.0.0.1", "MongoDB host")
var port = flag.Int("port", 27017, "MongoDB port")

func Test(t *testing.T) {
	TestingT(t)
}

type elasticTestSuite struct {
	c *Client
}

var _ = Suite(&elasticTestSuite{})

func (s *elasticTestSuite) SetUpSuite(c *C) {
    cfg := new(ClientConfig)
    cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
    cfg.Username = ""
    cfg.Password = ""
    s.c = NewClient(cfg)
}

func (s *elasticTestSuite) TearDownSuite(c *C) {

}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func (s *elasticTestSuite) TestBulk(c *C) {
	database := "dummy"
	collection := "comment"

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionInsert
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		req.Database = database
		req.Collection = collection
		items[i] = req
	}

	err := s.c.Bulk(items)
	c.Assert(err, IsNil)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.ID = id
		req.Database = database
		req.Collection = collection
		items[i] = req
	}
	err = s.c.Bulk(items)
	c.Assert(err, IsNil)

	items = make([]*BulkRequest, 3)

    req := new(BulkRequest)
    req.Action = ActionInsert
    req.ID = "1"
    req.Data = makeTestData(fmt.Sprintf("abc"), fmt.Sprintf("hello world?"))
	req.Database = database
	req.Collection = collection
    items[0] = req

    req = new(BulkRequest)
    req.Action = ActionUpdate
    req.ID = "1"
    req.Data = makeTestData(fmt.Sprintf("abc"), fmt.Sprintf("hello world!"))
	req.Database = database
	req.Collection = collection
    items[1] = req

    req = new(BulkRequest)
    req.Action = ActionDelete
    req.ID = "1"
	req.Database = database
	req.Collection = collection
    items[2] = req

    err = s.c.Bulk(items)
    c.Assert(err, IsNil)
}
