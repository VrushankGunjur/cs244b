#! /bin/bash

kill $(ps aux | grep nodeserver.py | grep -v grep | awk {'print $2'})