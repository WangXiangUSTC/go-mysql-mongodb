# go-mysql-mongodb ![test](https://github.com/WangXiangUSTC/go-mysql-mongodb/workflows/test/badge.svg)

go-mysql-mongodb is a service syncing your MySQL data into MongoDB automatically.

It uses `mysqldump` to fetch the origin data at first, then syncs data incrementally with binlog.

## Install

+ Install Go (1.6+) and set your [GOPATH](https://golang.org/doc/code.html#GOPATH)
+ `go get github.com/WangXiangUSTC/go-mysql-mongodb`, it will print some messages in console, skip it. :-)
+ cd `$GOPATH/src/github.com/WangXiangUSTC/go-mysql-mongodb`
+ `make`

## How to use?

+ Create tables in MySQL.
+ Config base, see the example config [river.toml](./etc/river.toml).
+ Set MySQL source in the config file, see [Source](#source) below.
+ Customize MySQL and MongoDB mapping rule in the config file, see [Rule](#rule) below.
+ Start `./bin/go-mysql-mongodb -config=./etc/river.toml` and enjoy it.

## Notice

+ binlog format must be **row**.
+ binlog row image must be **full** for MySQL, you may lose some field data if you update PK data in MySQL with minimal or noblob binlog row image. MariaDB only supports full row image.
+ Can not alter table format at runtime.
+ MySQL table which will be synced should have a PK(primary key), multi-columns PK is allowed now, e,g, if the PKs is (a, b), we will use "a:b" as the key. The PK data will be used as "\_id" in MongoDB. And you can also config the id's constituent part with other columns.
+ `mysqldump` must exist in the same node with go-mysql-mongodb, if not, go-mysql-mongodb will try to sync binlog only.
+ Don't change too many rows at the same time in one SQL.

## Source

In go-mysql-mongodb, you must decide which tables you want to sync into MongoDB in the source config.

The format in config file is below:

```
[[source]]
schema = "test"
tables = ["t1", t2]

[[source]]
schema = "test_1"
tables = ["t3", t4]
```

`schema` is the database name, and `tables` include the table that need to be synced.

## Rule

By default, go-mysql-mongodb will use MySQL table name as the MongoDB's database and collection name, use MySQL table field name as the MongoDB's field name.
e.g, if a table is named blog, the default database and collection in MongoDB are both named blog, if the table field is named title,
the default field name is also named title.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
database = "t"
collection = "t"

    [rule.field]
    mysql = "title"
    mongodb = "my_title"
```

In the example above, we will use a new database and collection both named "t" instead of the default "t1", and use "my_title" instead of the field name "title".

## Rule field types

In order to map a mysql column on different mongodb types you can define the field type as follows:

```
[[rule]]
schema = "test"
table = "t1"
database = "t"
collection = "t"

    [rule.field]
    // This will map column title to mongodb search my_title
    title="my_title"

    // This will map column title to mongodb search my_title and use the array type
    title="my_title,list"

    // This will map column title to mongodb search title and use the array type
    title=",list"
```

Modifier "list" will translates a MySQL string field like "a,b,c" on a MongoDB array type '{"a", "b", "c"}' this is especially useful if you need to use those fields on filtering on MongoDB.

## Wildcard table

go-mysql-mongodb only allows you determine which table to be synced, but sometimes, if you split a big table into multi sub tables, like 1024, table_0000, table_0001, ... table_1023, it is very hard to write rules for every table.

go-mysql-mongodb supports using wildcard table, e.g:

```
[[source]]
schema = "test"
tables = ["test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
database = "river"
collection = "river"
```

"test_river_[0-9]{4}" is a wildcard table definition, which represents "test_river_0000" to "test_river_9999", at the same time, the table in the rule must be the same as it.

In the above example, if you have 1024 sub tables, all tables will be synced into MongoDB with database "river" and collection "river".


## Filter fields

You can use `filter` to sync specified fields, like:

```
[[rule]]
schema = "test"
table = "tfilter"
database = "test"
collection = "tfilter"

# Only sync following columns
filter = ["id", "name"]
```

In the above example, we will only sync MySQL table tfiler's columns `id` and `name` to MongoDB. 

## Why write this tool?
At first, I use [tungsten-replicator](https://github.com/vmware/tungsten-replicator) to synchronize MySQL data to MongoDB, but I found this tool more cumbersome, especially when initializing data at the beginning, and needed to deploy at least two services(one master and one slave). Later, I use [go-mysql-elasticsearch](https://github.com/siddontang/go-mysql-elasticsearch) to sync MySQL data to Elasticsearch, I found this tool is very simple to use. So I rewrite this tool to synchronize MySQL data to MongoDB, and named it `go-mysql-mongodb`.


## Feedback

go-mysql-mongodb is still in development, and we will try to use it in production later. Any feedback is very welcome.
