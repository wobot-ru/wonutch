#!/bin/bash

# consider this parameter in prod env 
# JAVA_OPTS=-DPOOL_MAX=512
docker run -d -p 4444:4444 -e SE_OPTS="-timeout 0" --name selenium-hub selenium/hub