package mongodb

import (
	//"bytes"
	//"encoding/json"
	"fmt"
	//"io/ioutil"
	//"net/http"
	//"net/url"
    "github.com/ngaut/log"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
	//"github.com/juju/errors"
)

type Client struct {
	Addr string
    Username string
    Password string
    c *mgo.Session
}

type ClientConfig struct {
    Addr string
    Username string
    Password string
}

func NewClient(conf *ClientConfig) *Client {
	c := new(Client)
	c.Addr = conf.Addr
    c.Username = conf.Username
    c.Password = conf.Password
    if len(c.Username) > 0 && len(c.Password) > 0 {
        c.c, _ = mgo.Dial(fmt.Sprintf("mongodb://%s:%s@%s", c.Username, c.Password, c.Addr))
    } else {
        c.c, _ = mgo.Dial(fmt.Sprintf("mongodb://%s", c.Addr))
    }

	return c
}

type ResponseItem struct {
	ID      string                 `json:"_id"`
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	Version int                    `json:"_version"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
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
	ActionInsert  = "insert"
)

type BulkRequest struct {
	Action string
    Database  string
	Collection   string
	ID     string
    Filter map[string]interface{}
	Data map[string]interface{}
}


type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

type BulkResponseItem struct {
	Index   string          `json:"_index"`
	Type    string          `json:"_type"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Status  int             `json:"status"`
    //Error   json.RawMessage `json:"error"`
	Found   bool            `json:"found"`
}

func (c *Client) Bulk(items []*BulkRequest) ( error) {
    colDict := map[string]*mgo.Bulk{}
    var database string
    var collection string
    for _, item := range items {
        database = item.Database
        collection = item.Collection
        key := fmt.Sprintf("%s_%s", database, collection)
        if _, ok := colDict[key]; ok {
            //coll := c.c.DB(database).C(collection)
            //colDict[key] = coll.Bulk()
        } else {
            //log.Infof("database:%s, collection:%s", database, collection)
            coll := c.c.DB(database).C(collection)
            colDict[key] = coll.Bulk()
        }
	    switch item.Action {
        case ActionDelete:
            log.Infof("%s.%s remove id: %s", database, collection, item.ID)
            colDict[key].Remove(bson.M{"_id": item.ID})
            c.c.DB(database).C(collection).Bulk().Remove(bson.M{"_id": item.ID})
            c.c.DB(database).C(collection).Bulk().Run()
        case ActionUpdate:
            colDict[key].Upsert(bson.M{"_id": item.ID}, bson.M{"$set": item.Data})
        case ActionInsert:
            log.Infof("id: %s", item.ID)
            item.Data["_id"] = item.ID
            colDict[key].Insert(item.Data)


        }
    }
    for _, v := range colDict {
        _, _ = v.Run()
    }

	return nil
}

