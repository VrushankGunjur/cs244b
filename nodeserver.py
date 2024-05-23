import socket
import requests
import threading
import constants
from collections import OrderedDict
import time
import sys

class NodeServer:
    def __init__(self, host, port):
        # set up connection to master server
        self.cache = OrderedDict()
        self.host = host
        self.port = port
        self.from_master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.from_master.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # ??

        retry = 0
        while 1:
            try:
                self.from_master.bind((self.host, int(self.port) + retry))
                break
            except Exception as e:
                print(e)
                retry += 1
                continue
    
        self.from_master.listen(1)  # only talk to the master server
        self.cache_lock = threading.Lock()
        self.id = None
        
        self.run_server()
        #self.cur_obj_thread = threading.Thread(target=self.run_server())

    def __del__(self):
        self.from_master.close()
    
    def heartbeat(self):
        # send heartbeats to master server
        while True:
            to_master = socket.socket()
            to_master.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            to_master.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            to_master.connect(('localhost', constants.TO_MASTER_FROM_NODES))
            msg = str(self.port) # the port # is the heartbeat message
            msg += ','
            # potentially set a delimiter after port # to seperate from nodeID
            if self.id:
                msg += str(self.id)
            to_master.send(msg.encode())
            print("heartbeat sent")
            response = to_master.recv(constants.PKT_SIZE)
            if not self.id:
                self.id = str(response.decode())
            time.sleep(1)
            to_master.close()
        
    def respond(self):
        # listen for commands from master server
        while True:
            socket_to_master, _ = self.from_master.accept()
            try:
                data = socket_to_master.recv(constants.PKT_SIZE)
                # process the command
                print(f"Received command on node server {self.id}: ", data.decode())
                
                # check if in cache, if not, send request to web server. Respond to master server with response
                url = data.decode()
                self.cache_lock.acquire()
                if url in self.cache:
                    self.cache.move_to_end(url, last=True)
                    response_to_master = bytes("200", 'utf-8') + self.cache[url]
                    self.cache_lock.release()
                    print(f"Hit cache")
                else:
                    # MAKE REQUEST TO INTERNET
                    self.cache_lock.release()
                    response = requests.get(url)
                    response_body = response.content
                    status_code_bytes = bytes(str(response.status_code), 'utf-8')
                    if response.status_code == 200:
                        self.cache_lock.acquire()
                        self.cache[url] = response_body
                        if len(self.cache) > constants.MAX_CACHE_SIZE:
                            self.cache.popitem(last=False)  # Pop LRU item
                        self.cache_lock.release()
                    response_to_master = status_code_bytes + response_body
                    print(f"Went to internet")
                socket_to_master.sendall(response_to_master)
            except Exception as e:
                print(f"Error: {e}")
            finally:
                socket_to_master.close()

            # data = self.to_master.recv(PKT_SIZE)
            # print("Received data: ", data.decode())
    
    def run_server(self):
        # need to send heartbeats, and listen for commands from master server
        response_thread = threading.Thread(target=self.respond)
        heartbeat_thread = threading.Thread(target=self.heartbeat)

        heartbeat_thread.start()
        response_thread.start()

        print(f"Node server successfully started on port {self.port}")

        # join the threads once they return (blocking)
        heartbeat_thread.join()
        response_thread.join()

if __name__ == "__main__":
    NodeServer('localhost', sys.argv[1])