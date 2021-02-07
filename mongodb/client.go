package mongodb

import (
	"fmt"
	//"github.com/ngaut/log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Client struct {
	Addr     string
	Username string
	Password string
	c        *mgo.Session
}

type ClientConfig struct {
	Addr     string
	Username string
	Password string
}

func NewClient(conf *ClientConfig) (*Client, error) {
	var err error

	c := new(Client)
	c.Addr = conf.Addr
	c.Username = conf.Username
	c.Password = conf.Password
	if len(c.Username) > 0 && len(c.Password) > 0 {
		c.c, err = mgo.Dial(fmt.Sprintf("mongodb://%s:%s@%s", c.Username, c.Password, c.Addr))
	} else {
		c.c, err = mgo.Dial(fmt.Sprintf("mongodb://%s", c.Addr))
	}

	if err != nil {
		return nil, err
	}

	return c, nil
}

type ResponseItem struct {
	ID         string                 `json:"_id"`
	Database   string                 `json:"_database"`
	Collection string                 `json:"_collection"`
	Found      bool                   `json:"found"`
	Source     map[string]interface{} `json:"_source"`
}

type Response struct {
	Code int
	ResponseItem
}

// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionInsert = "insert"
)

type BulkRequest struct {
	Action     string
	Database   string
	Collection string
	ID         string
	Filter     map[string]interface{}
	Data       map[string]interface{}
}

type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

type BulkResponseItem struct {
	Database   string `json:"_index"`
	Collection string `json:"_type"`
	ID         string `json:"_id"`
	Status     int    `json:"status"`
	Found      bool   `json:"found"`
}

func (c *Client) Bulk(items []*BulkRequest) error {
	colDict := map[string]*mgo.Bulk{}
	var database string
	var collection string
	for _, item := range items {
		database = item.Database
		collection = item.Collection
		key := fmt.Sprintf("%s_%s", database, collection)
		if _, ok := colDict[key]; ok {
			// do nothing
		} else {
			coll := c.c.DB(database).C(collection)
			colDict[key] = coll.Bulk()
		}
		switch item.Action {
		case ActionDelete:
			colDict[key].Remove(bson.M{"_id": item.ID})
		case ActionUpdate:
			colDict[key].Upsert(bson.M{"_id": item.ID}, bson.M{"$set": item.Data})
		case ActionInsert:
			item.Data["_id"] = item.ID
			colDict[key].Upsert(bson.M{"_id": item.ID}, bson.M{"$set": item.Data})

		}
	}
	for _, v := range colDict {
		_, err := v.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) DeleteDB(database string) error {
	db := c.c.DB(database)
	if db == nil {
		return nil
	}
	return db.DropDatabase()
}

func (c *Client) Get(database, collection, id string) (*Response, error) {
	resp := new(Response)
	resp.ID = id
	resp.Database = database
	resp.Collection = collection

	var result [](map[string]interface{})
	err := c.c.DB(database).C(collection).Find(bson.M{"_id": id}).All(&result)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		resp.Found = false
		return resp, nil
	}

	resp.Found = true
	resp.Source = result[0]
	return resp, err
}
