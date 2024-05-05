import socket
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from constants import PKT_SIZE, NUM_CACHE_SERVERS, MASTER_PORT, CACHE1_PORT
import time
from collections import defaultdict

"""
Central Server listens for connections/client requests, checks if their request is in cache. 
If not, server makes a request to the web server, caches the response, and sends the response to the client.
Central Server also listens to communications from the cache shards to monitor status.
"""

cache_servers = defaultdict(int)
cache_id = 0
heartbeat_lock = threading.Lock()

def handle_heartbeats():
    # set up a socket on 5003, listen for heartbeats from cache servers
    # update the cache server list according to hearbeat data
    # respond to join request with ID
    heartbeat_lock.acquire()
    while 1:
        s = socket.socket()
        s.connect(('localhost', 5003))
        s.listen(NUM_CACHE_SERVERS)
        connection, client = s.accept()
        response = connection.recv(PKT_SIZE)
        if response == 'heartbeat':
              cache_servers[] - last_heartbeat = time.time() 

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
        print(response)

        # forward cache server response to client
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(response)
        #self.wfile.write(response.content)

def run_server():
    # start a thread to deal with all heartbeats in a loop (and this can handle managing cache servers)
    #heartbeat_thread = threading.Thread(target=handle_heartbeats)
    #heartbeat_thread.start()
    
    # then, start HTTP server
    server_address = ('localhost', MASTER_PORT)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Starting server on port 5001')
    httpd.serve_forever()

    #heartbeat_thread.join()

        
    # CODE TO BIND SERVER TO CLIENT AND SEND MESSAGE
    # host = socket.gethostname()
    # port = 5001

    # server_socket = socket.socket()
    # server_socket.bind((host, port))

    # server_socket.listen(5) # how many clients the server can listen to at the same time

    # client_socket, addr = server_socket.accept()
    # print("Connection from: " + str(addr))
    # client_socket.recv(PKT_SIZE)
    # client_socket.send("Hello from server".encode())


if __name__ == "__main__":
    run_server()