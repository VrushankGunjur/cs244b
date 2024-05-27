import threading
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
import time
import socket
from nodeserver import NodeServer
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import constants
from collections import defaultdict
from hashring import HashRing
import sys
from psutil import process_iter
from signal import SIGTERM # or SIGKILL

AM_LEADER = False
cur_leader_port = None
node_server_list = [None]

def getNewSocketAndPort():
    retry = 0
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    while True:
        try:
            s.bind(('localhost', int(5002) + retry))
            break
        except Exception as e:
            print(f"couldn't bind to port {5002 + retry} retrying...")
            retry += 1
            continue
    
    return s, 5002 + retry

def become_leader():
    print("become leader")
    #global AM_LEADER
    #AM_LEADER = True

    # kill the node server that was running (if it was running)


    while node_server_list[0] is None:
        #continue # busy wait
        print(node_server_list)
        time.sleep(0.05) # sleep for 1 second

    node_server_list[0].exit = True # should exit the loop
    print(f"LMAO SET EXIT ")
    node_server_list[0].from_master.close() # close the server

    # pick the ports to listen to and send to and advertise on the election node
    return getNewSocketAndPort()


#election = None
zk = None

def run_node_server():
    print("run node server")
    node_server_list[0] = NodeServer()
    # election should not be None here
    #master_port = election.contenders()[0]

    # print(f"election contenders {election.contenders()}")
    # print(f"length of contenders {len(election.contenders())}")
    # print(f"election contenders222 {election.contenders()}")

    # INTEGRATE: if I am the master, then return

    # election.contenders()[0] = "changed contender"
    # print(f"election contenders updated: {election.contenders()}")
    # if election.contenders()[0] == int(sys.argv[1]):
    #     return
    
    #master_port = election.contenders()[0]
    # this master port could be ourselves (if we are the only person in the election)

    # get the leader's port (to listen to) and the port to send mail to
    # leader_name, host, port to listen to, port to send to
    leader_name, to_leader_host, to_leader_port, client_request_port = zk.get('/election')[0].decode().split(",")
    node_server_list[0].startup(getNewSocketAndPort(), to_leader_host, to_leader_port)
    #node_server_list[0].startup(leader_host, leader_port_listen, leader_port_send)

def leader_election():
    print(f"leader_election function")
    # at the start, we don't set port information. We only set that if we become the leader
    my_name = zk.create('/election/contender', b',,,', ephemeral=True, sequence=True)
    print(my_name)
    zk.set(my_name, f"{my_name.split('/')[-1]},,,".encode())
    # my_name is this node's queue ID number (e.g. /election/contender0000000001)
    #print(zk.get(my_name)[0].decode())
    print(f'my_name is {my_name}')

    @zk.ChildrenWatch("/election")
    def watch_children(children):
        #print(election.contenders())
        #print(f"Children {children} hostname {hostname}")
        # if a new leader was elected, update the host and port to talk to new leader

        # If the leader wasn't the one that died, don't reset unnecessarily
        # Also don't reset if we are the leader

        children = zk.get_children("/election")
        if not children:
            return
        print(f"LMAO children {children}")
        new_leader = min(children)
        #print(f"new leader {new_leader}")
        new_leader_info = zk.get("/election/" + new_leader)[0].decode()
        print(f"LZJLKJALKDFJL; {zk.get('/election')[0].decode()}")
        leader_name, to_leader_host, to_leader_port, _ = new_leader_info.split(",")
        

        old_leader_name, _, _, _ = zk.get("/election")[0].decode().split(",")
        
        print(f"LMAO old_leader_name {old_leader_name} new leader name {leader_name}")

        if old_leader_name == leader_name:  # leader did not change
            return
        elif leader_name != my_name.split("/")[-1]:  # leader changed, but not me
            print("the leader changed, but not me")
            while not to_leader_port:
                time.sleep(0.5)
                _, to_leader_host, to_leader_port, _ = zk.get("/election")[0].decode().split(",")
            node_server_list[0].reset(to_leader_host, to_leader_port)  
            return
        
        # we are the new leader
        to_me_socket, to_me_port = become_leader()
        # TODO: change from localhost
        print(f"{leader_name} became new leader")

        # run on a different thread?
        run_master_server(to_me_socket, to_me_port, my_name.split("/")[-1])

    # TODO: don't busy wait (as much? sleep?)
    while 1:
        time.sleep(1)
        print(f"waiting")


def main():
    global cur_leader_port
    global election
    global zk
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

    if not zk.exists("/election"):
        zk.create('/election', b',,,')
        print(f"created /election with value {zk.get('/election')}")

    # Create an election object
    #election = Election(zk, "/election_path", identifier=sys.argv[1])

    # Define the hostname to differentiate nodes
    hostname = socket.gethostname()    
    
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



node_servers = defaultdict(int)     # key: node_id, value: last time heartbeat was received
node_id_to_port = defaultdict(int)  # key: node_id, value: port number
next_node_server_id = 0             # id to assign to next cache server that sends a heartbeat
server_map_lock = threading.Lock()  # lock for node_servers and node_id_to_port
hash_ring = HashRing()              # hash ring to determine which cache server to send request to
hash_ring_lock = threading.Lock()   # lock for hash_ring

from_node = None

def receive_heartbeats(to_master_socket, to_master_port):
    global from_node
    global next_node_server_id
    # set up a socket on 5003, listen for heartbeats from cache servers
    # update the cache server list according to hearbeat data

    # host = socket.gethostname()
    from_node = to_master_socket
    # from_node = socket.socket()
    # from_node.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    #from_node.bind(('localhost', constants.TO_MASTER_FROM_NODES))
    print(f"Master listening to node servers on port {to_master_port}...")
    from_node.listen(constants.NUM_CACHE_SERVERS) # how many clients the server can listen to at the same time

    while True:
        connection, ephemeral_addr = from_node.accept()
        print("Connection from: " + str(ephemeral_addr))
        response = connection.recv(constants.PKT_SIZE)
        #connection.send("Hello from server".encode())
        response = response.decode().split(",")         # response is formatted as nodePort,nodeId
        nodePort = int(response[0])
        nodeId = response[1]

        # assign an id if server doesn't yet have one
        print("received heartbeat from node server")
        if nodeId == '':
            incoming_port = nodePort
            print(f"Locking server_map_lock")
            with server_map_lock:
                node_id_to_port[next_node_server_id] = incoming_port
                node_servers[next_node_server_id] = time.time()
            print(f"Released server_map_lock")
            print(f"Locking hash_ring_lock")
            with hash_ring_lock:
                hash_ring.add_node(next_node_server_id)
            print(f"Released hash_ring_lock")
            connection.sendall(str(next_node_server_id).encode())
            next_node_server_id += 1
            print("assigning node id...")
        else:
            print(f"Locking server_map_lock")
            with server_map_lock:
                node_servers[int(nodeId)] = time.time()
            print(f"Released server_map_lock")
            connection.sendall("".encode())
        connection.close()

# Flush caches that haven't sent heartbeats recently (this cleanup isn't working right now, see above @viraj)
def flush():
    while True:
        for node_id in list(node_servers.keys()):
            if time.time() - node_servers[node_id] > constants.HEARTBEAT_EXPIRATION_TIME:
                print("node removed")
                print(f"Locking server_map_lockk")
                with server_map_lock:
                    del node_servers[node_id]
                    del node_id_to_port[node_id]
                print(f"Released server_map_lock")

                with hash_ring_lock:
                    hash_ring.remove_node(node_id)
        time.sleep(constants.HEARTBEAT_INTERVAL * 2)

"""
 Hash the URL to determine which cache server to send request to, out of the running
 list of node servers we maintain through the heartbeat system. 

 What to do if the cache server is down/no response? Remove it from the server list, recalculate
 target cache server and send the request out.

 Throw an error if there are no cache servers available (length of cache server list is 0)a
"""
class RequestHandler(BaseHTTPRequestHandler):

    # Proxy handles GET request from client
    def do_GET(self):
        # figure out which cache server to forward request to, and send
        
        #print(self.request)
        # print("Received request from client")
        # print(self.path)
        # r = requests.get(self.path)
        # print(r.content)

        # forward request to cache server
        requested_url = self.path
        print(f"Locking hash_ring_lock")
        with hash_ring_lock:
            node_id = hash_ring[requested_url]
        print(f"Release hash_ring_lock")

        print(f"Locking server_map_lock")
        with server_map_lock:
            node_port = node_id_to_port[node_id]
        print(f"Release server_map_lock")

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.connect(('localhost', node_port))
            s.send(self.path.encode())

            # receive response from cache server
            response = recv_all(s)
            response_string = response.decode('utf-8')
            try:
                status_code = int(response_string[:3])
                response_body = bytes(response_string[3:], 'utf-8')
            except Exception as e:
                status_code = -1
                response_body = b''      
                print(f"error {e} in response from cache server")  
            
            # forward cache server response to client
            self.send_response(status_code)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(response_body)
        except Exception as e:
            print(f"Error in master server: {e}")
            self.send_error(500)
        finally:
            s.close()

def recv_all(sock, buffer_size=4096):
    data = b''
    while True:
        part = sock.recv(buffer_size)
        data += part
        if len(part) < buffer_size:
            # If part is less than buffer_size, we assume it's the end of the data
            break
    return data

def run_master_server(to_me_socket, to_me_port, my_name):
    # start a thread to deal with all heartbeats in a loop (and this can handle managing cache servers)
    #to_master_socket_and_port = getNewSocketAndPort()
    heartbeat_thread = threading.Thread(target = receive_heartbeats, args=(to_me_socket, to_me_port))
    heartbeat_thread.start()

    # start a thread to flush nodes that haven't sent heartbeats
    flush_thread = threading.Thread(target = flush)
    flush_thread.start()

    # # free up the master port if it's already in use
    # print('freeing port 5001')
    # for proc in process_iter():
    #     for conns in proc.connections(kind='inet'):
    #         if conns.laddr.port == constants.MASTER_PORT:
    #             proc.send_signal(SIGTERM) # or SIGKILL
    # print(f"freed port 5001")

    # then, start HTTP server that the client communicates with
    cur_port = constants.MASTER_PORT

    while 1:
        try:
            server_address = ('localhost', cur_port)
            httpd = HTTPServer(server_address, RequestHandler)
            print(f'Starting master server on port {cur_port}')
            zk.set('/election', f"{my_name},localhost,{to_me_port},{cur_port}".encode())
            # httpd = HTTPServer(server_address, RequestHandler)
            # print('Starting master server on port 5001')
            httpd.serve_forever()
            break
        except Exception as e:
            print(f"Error in starting master server on port {cur_port}: {e}")
            cur_port += 1

    # server_address = ('localhost', constants.MASTER_PORT)
    # httpd = HTTPServer(server_address, RequestHandler)
    # print('Starting master server on port 5001')
    # httpd.serve_forever()

    heartbeat_thread.join()
    flush_thread.join()


if __name__ == "__main__":
    main()