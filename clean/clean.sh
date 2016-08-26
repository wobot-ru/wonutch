#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: clean.sh <segments prefix>"
    echo -e
    exit 1
fi

PREF="$1"

for i in $(hadoop fs -find crawl/segments -name ${PREF}*); do 
    echo "Cleaning segment: ""$i"
    hadoop fs -rm -f -R -skipTrash "$i/crawl_parse" "$i/parse*"
done

