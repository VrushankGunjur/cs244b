#! /bin/bash
start_port=5003
num_nodes=3

for ((i=0; i<$num_nodes; i++))
do 
    port=$((start_port+i))
    echo "Starting node $i on port $port"
    python3 zookeeperNode.py $port &
    sleep 1
done
