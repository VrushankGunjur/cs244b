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
if not zk.exists("/election"):
    zk.create('/election', b'')


# election = Election(zk, "/election_path")

# Define the hostname to differentiate nodes
hostname = socket.gethostname()

my_id = None




def leader_election():
    # run for election
    global my_id
    # Election process
    @zk.ChildrenWatch("/election")
    def watch_children(children):
        print(f"Children {children} hostname {hostname}")

        # if we are the leader (have the lowest sequence number), then run master code
        children = zk.get_children("/election")
        print(children)
        if not children:
            return
        
        leader = min(zk.get_children("/election"))
        if leader == my_id:
            print("I am the leader")
            become_leader()
        
        
        print(f'pure leader name is {leader}')
        print("in watch, leader is "+zk.get("/election/" + leader)[0].decode())
        # if hostname in children:
        #     print(f"{hostname} is participating in the election")
        # else:
        #     print(f"{hostname} is not participating in the election")
    
    print(f"leader_election function")
    path = zk.create('/election/contender', b'me', ephemeral=True, sequence=True)
    # path is this node's queue ID number (e.g. /election/contender0000000001)
    my_id = path.split('/')[-1]
    zk.set(path, b'set data')
    print(zk.get(path)[0].decode())
    print(f'path is {path}')

    # TODO: don't busy wait (as much? sleep?)
    while 1:
        time.sleep(1)
        print(f"waiting")
    #election.run(become_leader)

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


