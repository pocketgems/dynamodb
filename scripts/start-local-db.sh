#!/bin/bash
cd "`dirname \"$0\"`"

if [ "`curl -s http://localhost:8000`" == "" ] || [ $# -ne 0 ]; then
    COMPOSE_PROJECT_NAME='dynamodb' docker-compose up -d $@
fi
