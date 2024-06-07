#! /bin/bash
start_port=5003
num_nodes=3

for ((i=0; i<$num_nodes; i++))
do 
    echo "Starting node $i"
    python3 zookeeperNode.py &
    sleep 1
done
