#!/bin/bash

bin="`dirname "$0"`"
bin="`cd "$bin"; pwd`"

# determines whether need to run selenium grid
selenium=false
if [ -f "${bin}"/../../selenium-grid/nodes.txt ]; then
    selenium=true
fi

if $selenium; then
    echo "Starting selenium grid ..."
    for HOST in $(cat "${bin}/../../selenium-grid/nodes.txt" );
    do
        echo $HOST "found";
        ssh $HOST "start-grid.sh 10" ;
    done
fi

if $selenium; then
    echo "Stopping selenium grid ..."
    for HOST in $(cat "${bin}/../../selenium-grid/nodes.txt" );
    do
        echo $HOST "found";
        ssh $HOST "stop-grid.sh";
    done
fi
