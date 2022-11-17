#!/bin/bash
set -o errexit
set -o nounset

cd "`dirname \"$0\"`"
cd ../..

files=`find . -name '*.js' -not -path '*node_modules*' -not -path '*generated*'`

echo -e Running standard at `date` ...
node_modules/standard/bin/cmd.js --verbose $files | node_modules/snazzy/bin/cmd.js
echo -e Running eslint at `date` ...
node_modules/eslint/bin/eslint.js $files
