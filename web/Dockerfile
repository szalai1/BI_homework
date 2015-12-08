FROM golang:latest

RUN go get gopkg.in/mgo.v2

COPY bi.go bi.go

ENTRYPOINT go run bi.go
EXPOSE "9090"    