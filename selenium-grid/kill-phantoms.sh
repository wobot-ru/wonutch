#!/bin/bash

kill $(ps aux | grep phantomjs | grep -v grep | awk '{print $2}')