import socket
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import constants
import time
from collections import defaultdict
from hashring import HashRing

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

def receive_heartbeats():
    global next_node_server_id
    # set up a socket on 5003, listen for heartbeats from cache servers
    # update the cache server list according to hearbeat data

    # host = socket.gethostname()

    from_node = socket.socket()
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
            print(f"Locking server_map_lockk")
            server_map_lock.acquire()
            node_id_to_port[next_node_server_id] = incoming_port
            node_servers[next_node_server_id] = time.time()
            server_map_lock.release()
            print(f"Released server_map_lock")
            print(f"Locking hash_ring_lock")
            hash_ring_lock.acquire()
            hash_ring.add_node(next_node_server_id)
            hash_ring_lock.release()
            print(f"Released hash_ring_lock")
            connection.sendall(str(next_node_server_id).encode())
            next_node_server_id += 1
            print("assigning node id...")
        else:
            print(f"Locking server_map_lock")
            server_map_lock.acquire()
            node_servers[int(nodeId)] = time.time()
            server_map_lock.release()
            print(f"Released server_map_lock")
            connection.sendall("".encode())
        connection.close()

        # this flush only runs after the blocking call to 'accept', so it might not actually remove failed caches on time
        # @viraj
        flush()

# Flush caches that haven't sent heartbeats recently (this cleanup isn't working right now, see above @viraj)
def flush():
    for node_id in list(node_servers.keys()):
        if time.time() - node_servers[node_id] > constants.MAX_HEARTBEAT_TIME:
            print("node removed")
            print(f"Locking server_map_lockk")
            server_map_lock.acquire()
            del node_servers[node_id]
            del node_id_to_port[node_id]
            server_map_lock.release()
            print(f"Released server_map_lock")

            hash_ring_lock.acquire()
            hash_ring.remove_node(node_id)
            hash_ring_lock.release()

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
        hash_ring_lock.acquire()
        node_id = hash_ring[requested_url]
        hash_ring_lock.release()
        print(f"Release hash_ring_lock")

        print(f"Locking server_map_lockk")
        server_map_lock.acquire()
        node_port = node_id_to_port[node_id]
        server_map_lock.release()
        print(f"Release server_map_lockk")

        s = socket.socket()
        s.connect(('localhost', node_port))
        s.send(self.path.encode())

        # receive response from cache server
        response = s.recv(constants.PKT_SIZE)
        response_string = response.decode('utf-8')
        try:
            status_code = int(response_string[:3])
            response_body = bytes(response_string[3:], 'utf-8')
        except Exception as e:
            status_code = -1
            response_body = b''      
            print(f"LMAO ERROR {e}")  
        
        # forward cache server response to client
        self.send_response(status_code)
        self.send_header('Cstatus_codeent-type', 'text/html')
        self.end_headers()
        self.wfile.write(response_body)
        #self.wfile.writee.content)

def run_server():
    # start a thread to deal with all heartbeats in a loop (and this can handle managing cache servers)
    heartbeat_thread = threading.Thread(target = receive_heartbeats)
    heartbeat_thread.start()
    
    # then, start HTTP server
    server_address = ('localhost', constants.MASTER_PORT)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Starting server on port 5001')
    httpd.serve_forever()

    heartbeat_thread.join()

if __name__ == "__main__":
    run_server()