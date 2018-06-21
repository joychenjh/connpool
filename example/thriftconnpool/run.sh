#!/usr/bin/env bash


thrift --gen go  example.thrift

go fmt ./gen-go/example/*go