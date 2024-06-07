#! /bin/bash

kill $(ps aux | grep zookeeperNode.py | grep -v grep | awk {'print $2'})
#kill $(ps aux | grep masterserver.py | grep -v grep | awk {'print $2'})
