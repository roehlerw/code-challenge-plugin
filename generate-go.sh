#!/usr/bin/env bash

protoc -I .  --go_out=plugins=grpc:plugin plugin.proto
go fmt plugin/plugin.pb.go