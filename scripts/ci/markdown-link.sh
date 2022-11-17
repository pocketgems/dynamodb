#!/bin/bash
set -o errexit
set -o nounset

cd "`dirname \"$0\"`"
cd ../..

./node_modules/remark-cli/cli.js -q -f -u validate-links .
