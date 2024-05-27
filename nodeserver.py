import socket
import requests
import threading
import constants
from collections import OrderedDict
import time
import sys

class NodeServer:
    def __init__(self):
        # set up connection to master server
        self.cache = OrderedDict()
        
        self.from_master = None
        self.cache_lock = threading.Lock()
        self.id = None
        #self.cur_obj_thread = threading.Thread(target=self.run_server())

    def startup(self, sock_and_port, to_master_host, to_master_port):
        self.from_master, self.port = sock_and_port
        #self.from_master.bind('localhost', self.port)
        self.from_master.listen(1)
        self.reset(to_master_host, to_master_port)
        self.run_server()
    
    def __del__(self):
        if self.from_master is not None:
            self.from_master.close()
    
    def reset(self, new_to_master_host, new_to_master_port):

        self.to_master_host = new_to_master_host
        self.to_master_port = new_to_master_port
        self.exit = False
        self.id = ''
        
    def heartbeat(self):
        # send heartbeats to master server
        while not self.exit:
            try:
                print(f"trying to send heartbeat")
                to_master = socket.socket()
                to_master.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                to_master.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                to_master.connect((self.to_master_host, int(self.to_master_port)))

                # TODO: send our hostname as well
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
            except Exception as e:
                print(f'error in heartbeat {e}')
                time.sleep(1)
                pass
        
    def respond(self):
        # listen for commands from master server
        while not self.exit:
            # this blocks until a connection is made or the socket is closed
            print(f"in respond, from_master is {self.from_master.getsockname()}")
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