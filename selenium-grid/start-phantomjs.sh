#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: start-phantomjs.sh <Num phantomjs instances>"
    echo -e
    exit 1
fi

CNT="$1"

for ((i=1; i <= CNT ; i++))
do

  docker run -d -e "PHANTOMJS_OPTS=--ignore-ssl-errors=true --web-security=no --ssl-protocol=any" --link selenium-hub:hub akeem/selenium-node-phantomjs

done

exit 0
