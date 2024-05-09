import socket
import requests
import threading
from constants import PKT_SIZE, MAX_CACHE_SIZE, TO_MASTER_FROM_NODES
from collections import OrderedDict
import time

class NodeServer:
    def __init__(self, host, port):
        # set up connection to master server
        self.cache = OrderedDict()
        self.host = host
        self.port = port
        self.from_master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.from_master.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # ??
        self.from_master.bind((self.host, self.port))
        self.from_master.listen(1)  # only talk to the master server

        self.server_socket = socket.socket()
        self.server_socket.connect(('localhost', TO_MASTER_FROM_NODES))
        self.cache_lock = threading.Lock()
        self.id = None
        
        self.run_server()

    def heartbeat(self):
        # send heartbeats to master server
        while True:
            msg = "heartbeat"
            if self.id:
                msg += str(self.id)
            self.server_socket.send(msg.encode())
            print("heartbeat sent")
            response = self.server_socket.recv(PKT_SIZE)
            if not self.id:
                self.id = str(response)
                print('node ID added')
            time.sleep(5)
        
    def respond(self):
        # listen for commands from master server
        while True:
            socket_to_master, addr = self.from_master.accept()
            data = socket_to_master.recv(PKT_SIZE)
            # process the command
            print("Received command: ", data.decode())
            
            # check if in cache, if not, send request to web server. Respond to master server with response
            url = data.decode()
            self.cache_lock.acquire()
            if url in self.cache:
                self.cache.move_to_end(url, last=True)
                response_to_master = self.cache[url]
                self.cache_lock.release()
            else:
                # MAKE REQUEST TO INTERNET
                self.cache_lock.release()
                response = requests.get(url)
                response_body = response.content
                status_code_bytes = bytes(str(response.status_code), 'utf-8')
                if response.status_code == 200:
                    self.cache_lock.acquire()
                    self.cache[url] = response_body
                    if len(self.cache) > MAX_CACHE_SIZE:
                        self.cache.popitem(last=False)  # Pop LRU item
                    self.cache_lock.release()
                response_to_master = status_code_bytes + response_body

            socket_to_master.send(response_to_master)
            socket_to_master.close()

            # data = self.server_socket.recv(PKT_SIZE)
            # print("Received data: ", data.decode())
    
    def run_server(self):
        # need to send heartbeats, and listen for commands from master server
        response_thread = threading.Thread(target=self.respond)
        heartbeat_thread = threading.Thread(target=self.heartbeat)

        heartbeat_thread.start()
        response_thread.start()

        # join the threads once they return (blocking)
        heartbeat_thread.join()
        response_thread.join()

if __name__ == "__main__":
    node_server = NodeServer('localhost', 5002)
    print("Node server started on port 5001")