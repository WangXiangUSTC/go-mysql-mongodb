FROM golang:alpine

MAINTAINER WangXiangUSTC

COPY . /go/src/github.com/WangXiangUSTC/go-mysql-mongodb

RUN cd /go/src/github.com/siddontang/go-mysql-mongodb/ && \
    go build -o bin/go-mysql-mongodb ./cmd/go-mysql-mongodb && \
    cp -f ./bin/go-mysql-mongodb /go/bin/go-mysql-mongodb

ENTRYPOINT ["go-mysql-mongodb"]

