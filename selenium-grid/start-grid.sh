#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: start-phantomjs.sh <Num phantomjs instances>"
    echo -e
    exit 1
fi

# consider this parameter in prod env 
# JAVA_OPTS=-DPOOL_MAX=512
docker run -d -p 4444:4444 -e SE_OPTS="-timeout 0 -maxSession 10 -newSessionWaitTimeout 3000" --name selenium-hub selenium/hub

CNT="$1"

for ((i=1; i <= CNT ; i++))
do
    docker run -d -e "PHANTOMJS_OPTS=--ignore-ssl-errors=yes --web-security=no --ssl-protocol=any" --link selenium-hub:hub akeem/selenium-node-phantomjs
done

exit 0
