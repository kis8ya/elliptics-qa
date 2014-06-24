#!/bin/bash
DOCPATH=$1
export PYTHONPATH='lib'
for i in $(find tests/ -name 'test*.py'); do
  AUXPATH=$(dirname $i)
  DOCFILE="$DOCPATH/$(basename $i).rst"
  if [ -e $DOCFILE ]; then
    echo "Warning: file $DOCFILE already exists. Overwriting.">&2
  fi
  PYTHONPATH="$PYTHONPATH:$AUXPATH" pdoc $i > "$DOCFILE"
done
