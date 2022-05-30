#!/bin/bash
docker kill $(docker ps -q)
docker rm $(docker ps -a -q)

docker run -d --restart unless-stopped --name signaldb -e "POSTGRES_PASSWORD=postgres" -e "POSTGRES_DB=signal_abuses" -p 5432:5432 postgres:latest
docker run -d --restart unless-stopped --name redis -e "IP=0.0.0.0" -p 7000-7005:7000-7005 grokzen/redis-cluster:latest
docker run -d --restart unless-stopped --name signal_dynamo -p 8000:8000 amazon/dynamodb-local:latest

docker container ls
