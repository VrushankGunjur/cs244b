import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import constants
import time
from collections import defaultdict
from hashring import HashRing
import os
import json

"""
    Node management plane
    Client service plane
"""

"""
Central Server listens for connections/client requests, checks if their request is in cache. 
If not, server makes a request to the web server, caches the response, and sends the response to the client.
Central Server also listens to communications from the cache shards to monitor status.
"""

node_servers = defaultdict(int)     # key: node_id, value: last time heartbeat was received
node_id_to_port = defaultdict(int)  # key: node_id, value: port number
next_node_server_id = 0             # id to assign to next cache server that sends a heartbeat
server_map_lock = threading.Lock()  # lock for node_servers and node_id_to_port
hash_ring = HashRing()              # hash ring to determine which cache server to send request to
hash_ring_lock = threading.Lock()   # lock for hash_ring

load_distribution_json = 'load_distribution.json' 
website_to_node_json = 'website_to_node.json'
load_distribution = defaultdict(int) # key: node_id, value: number of cache requests
website_to_node = defaultdict(int)  # key: website, value: node_id 
load_distribution[-1] = 0

def read_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            return json.load(file)
    else:
        return {}

# Update the json entry at key of node_id with the new request
def update_json(filepath, node_id_to_request):
    with open(filepath, 'w') as file:
        json.dump(node_id_to_request, file, indent = 0)

def receive_heartbeats():
    global next_node_server_id
    # set up a socket on 5003, listen for heartbeats from cache servers
    # update the cache server list according to hearbeat data

    # host = socket.gethostname()

    from_node = socket.socket()
    from_node.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    from_node.bind(('localhost', constants.TO_MASTER_FROM_NODES))
    print(f"Master listening to node servers on port {constants.TO_MASTER_FROM_NODES}...")
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
            nodeID = next_node_server_id
        else:
            print(f"Locking server_map_lock")
            with server_map_lock:
                node_servers[int(nodeId)] = time.time()
            print(f"Released server_map_lock")
            connection.sendall("".encode())
        connection.close()

# Flush caches that haven't sent heartbeats recently
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
 TODO: hash the URL to determine which cache server to send request to, out of the running
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

        print(f"Locking server_map_lockk")
        with server_map_lock:
            node_port = node_id_to_port[node_id]
        print(f"Release server_map_lockk")

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.connect(('localhost', node_port))
            s.send(self.path.encode())

            # Update load distribution dict for load balance testing
            load_distribution[-1] += 1
            if node_id in load_distribution:
                load_distribution[node_id] += 1
            else:
                load_distribution[node_id] = 1
            website_to_node[requested_url] = node_id

            if load_distribution[-1] == 1000:
                update_json(load_distribution_json, load_distribution)
                update_json(website_to_node_json, website_to_node)

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

def run_server():
    # start a thread to deal with all heartbeats in a loop (and this can handle managing cache servers)
    heartbeat_thread = threading.Thread(target = receive_heartbeats)
    heartbeat_thread.start()

    # start a thread to flush nodes that haven't sent heartbeats
    flush_thread = threading.Thread(target = flush)
    flush_thread.start()
    
    # then, start HTTP server
    server_address = ('localhost', constants.MASTER_PORT)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Starting server on port 5001')
    httpd.serve_forever()

    heartbeat_thread.join()
    flush_thread.join()

if __name__ == "__main__":
    run_server()