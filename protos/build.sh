#!/bin/bash


protoc --proto_path=. \
    --go_out=../golang/. \
    *.proto

protoc --proto_path=. \
    --go-grpc_out=../golang/. \
    *.proto