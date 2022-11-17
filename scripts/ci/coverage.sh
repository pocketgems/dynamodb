#!/bin/bash
set -o errexit
set -o nounset

cd "`dirname \"$0\"`"/../..

./node_modules/jest/bin/jest.js --coverage --config=./jest.config.json
