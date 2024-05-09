import socket
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from constants import PKT_SIZE, NUM_CACHE_SERVERS, MASTER_PORT, CACHE1_PORT, HEARTBEAT_MSG_LEN, MAX_HEARTBEAT_TIME, TO_MASTER_FROM_NODES
import time
from collections import defaultdict

"""
Central Server listens for connections/client requests, checks if their request is in cache. 
If not, server makes a request to the web server, caches the response, and sends the response to the client.
Central Server also listens to communications from the cache shards to monitor status.
"""

cache_servers = defaultdict(int) # this dict is going to have to log the port number of the cache server?
cache_id = 0
server_map_lock = threading.Lock()

def receive_heartbeats():
    # set up a socket on 5003, listen for heartbeats from cache servers
    # update the cache server list according to hearbeat data

    host = socket.gethostname()
    port = TO_MASTER_FROM_NODES

    server_socket = socket.socket()
    server_socket.bind((host, port))

    server_socket.listen(NUM_CACHE_SERVERS) # how many clients the server can listen to at the same time

    while True:
        connection, addr = server_socket.accept()
        print("Connection from: " + str(addr))
        response = connection.recv(PKT_SIZE)
        #connection.send("Hello from server".encode())
        if "heartbeat" in response:
            # assign an id if server doesn't yet have one
            print("received heartbeat from node server")
            if len(response) == HEARTBEAT_MSG_LEN:
                cache_id += 1
                connection.sendall(str(cache_id).encode())
                print("assigning node id...")
            else:
                curr_id = int(response[HEARTBEAT_MSG_LEN + 1:])
                cache_servers[curr_id] = time.time()
                connection.sendall("".encode())
        flush()

# Flush caches that haven't sent heartbeats recently
def flush():
    for cache_id in cache_servers.keys():
        if time.time() - cache_servers[cache_id] > MAX_HEARTBEAT_TIME:
            print("node removed")
            del cache_id

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
        s = socket.socket()
        s.connect(('localhost', CACHE1_PORT))
        s.send(self.path.encode())

        # receive response from cache server
        response = s.recv(PKT_SIZE)
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
    server_address = ('localhost', MASTER_PORT)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Starting server on port 5001')
    httpd.serve_forever()

    heartbeat_thread.join()

if __name__ == "__main__":
    run_server()