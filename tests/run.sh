#!/usr/bin/env bash

set -eu

function EXEC_SQL() {
    mysql -utest -h 127.0.0.1 -psecret -P3306 -e "$1"
}

echo "Prepare data in MySQL"
EXEC_SQL "drop database if exists go_mysql_mongodb_test"
EXEC_SQL "create database go_mysql_mongodb_test";
EXEC_SQL "create table go_mysql_mongodb_test.t_0001(id int primary key, name varchar(10));";
EXEC_SQL "insert into go_mysql_mongodb_test.t_0001 values(1, 'a');"

echo "Start go-mysql-mongodb"
./bin/go-mysql-mongodb --config ./tests/river.toml > test.log 2>&1 &

echo "Insert data into MySQL"
EXEC_SQL "insert into go_mysql_mongodb_test.t_0001 values(2, 'b');"
EXEC_SQL "insert into go_mysql_mongodb_test.t_0001 values(3, 'c');"

echo "Check data in MongoDB"
cat test.log
mongo go_mysql_mongodb_test --quiet --eval 'db.t_0001.find().toArray()' > find.result
# output:
#[
#	{
#		"_id" : "1",
#		"id" : NumberLong(1),
#		"name" : "a"
#	},
#	{
#		"_id" : "2",
#		"id" : NumberLong(2),
#		"name" : "b"
#	},
#	{
#		"_id" : "3",
#		"id" : NumberLong(3),
#		"name" : "c"
#	}
#]
./tests/check_contains  '"_id" : "1"' find.result
./tests/check_contains  '"_id" : "2"' find.result
./tests/check_contains  '"_id" : "3"' find.result
./tests/check_contains  '"name" : "a"' find.result
./tests/check_contains  '"name" : "b"' find.result
./tests/check_contains  '"name" : "c"' find.result
