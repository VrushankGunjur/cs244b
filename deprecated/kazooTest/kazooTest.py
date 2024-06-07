import threading
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
import time
import socket

AM_LEADER = False





def become_leader():
    print("become leader")
    global AM_LEADER
    AM_LEADER = True
    t1 = threading.Thread(target=leader_thread_1)
    t1.start()
    t1.join()

def run_node_server():
    print("run node server")
    t1 = threading.Thread(target=follower_thread_1)
    t1.start()
    t1.join()

def leader_thread_1():
    while True:
        print("Leader Thread 1 is running")
        time.sleep(1)

def follower_thread_1():
    while not AM_LEADER:
        print("Follower Thread 1 is running")
        time.sleep(1)


zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

# Create an election object
election = Election(zk, "/election_path")

# Define the hostname to differentiate nodes
hostname = socket.gethostname()

# Election process
@zk.ChildrenWatch("/election_path")
def watch_children(children):
    if hostname in children:
        print(f"{hostname} is participating in the election")
    else:
        print(f"{hostname} is not participating in the election")

def leader_election():
    print(f"leader_election function")
    election.run(become_leader)

# Participate in the election
try:
    election_thread = threading.Thread(target=leader_election)

    node_server_thread = threading.Thread(target=run_node_server)

    election_thread.start()
    node_server_thread.start()

    election_thread.join()
    node_server_thread.join()

except Exception as e:
    print(f"Exception in leader election: {e}")
finally:
    zk.stop()


