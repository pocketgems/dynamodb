#!/bin/bash
cd "`dirname \"$0\"`"
script_path="`pwd`"

cd ..
embed-markdown # Update .md files before embedding the .md files into docs

cd "$script_path"
rm -rf generated
npx jsdoc --configure jsdoc.config.json `find ../src -name '*.js' -type f`
