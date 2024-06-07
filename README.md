# Scalable Fault-Tolerant Web Caching with Consistent Hashing

We present a Python implementation of a web cache that makes use of consistent hashing for dynamic load balancing. 

There are two branches, `main` and `zookeeper`. The `main` branch contains code for our baseline implementation of web caching that makes use of consistent hashing to allow for dynamic availability of cache servers without impacting performance. However, the implementation on the main branch relies on a strong master. If this master dies, the system shuts down.

To address this issue, we implement leader elections in the `zookeeper` branch. We make use of a Python library called `Kazoo` to interface with Apache Zookeeper. Since the Election recipe in Kazoo doesn't do what we need it to (namely, publish network information to redirect all members of the cluster), we implement our own mechanism for elections described in the paper.

## Running the Web Cache

### main branch

first, start the master server
```
python3 masterserver.py
```

then, in a seperate terminal, spin up the node servers. The number of nodes is specified in the script.
```
sh init_nodes.sh
```

to send a request,
```
python3 client.py [http web url]
```

When finished, use the helper shell script to spin down the node servers cleanly
```
sh shutdown_nodes.sh
```

### zookeeper branch
A seperate master is now gone, just run 
```
sh init_nodes.sh
```

and use the client as normal. The `zookeeper` branch client will connect to the Zookeeper server to find out who the current master is automatically before making the request.

As always
```
sh shutdown_nodes.sh
```
to ensure a clean shutdown

## File Structure

`client.py` simulates a web request and hits the web cache as a proxy.

`constants.py` contains relevant constants shared across files 

`evalmetrics.py`, `load_distribution.json`, `testhashring.py`, and `website_to_node.json` are used internally for testing

`init_nodes.sh` is a helper script to spin up node servers, `shutdown_nodes.sh` is the corresponding cleanup script

`masterserver.py` is the code for a strong master

`nodeserver.py` contains the code for cache nodes in the baseline implementation

In the zookeeper branch, the additional `zookeeperNode.py` file contains the bulk of the code for a multipurpose cluster node that participates in elections, and toggles between master and follower as required.
