import requests 
import time
import sys
from kazoo.client import KazooClient
# Client code makes a request for a webpage, rerouting through server (proxy)

proxies = {
    "http": "http://localhost:5004",
    "https": "http://localhost:5001"
}

def run_client():
    #url = "http://www.stanford.edu"
    # url = "http://www.google.com"
    #url = "http://www.yahoo.com"
    url = "http://www.google.com"
    if len(sys.argv) > 1:
        url = sys.argv[1]
    
    global zk
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

	# client gets current master
	newport = ''
	while newport == '':
	    newport = zk.get("/election")[0].decode().split(",")[3]
	
    proxies["http"] = "http://localhost:" + newport
    r = requests.get(url, proxies=proxies)
    print(r.text)


    # host = socket.gethostname()
    # port = 5001

    # client_socket = socket.socket()
    # client_socket.connect((host, port))

    # client_socket.send("Hello from client".encode())
    # print(client_socket.recv(1024))


def multi_request():
    start_time = time.time()
    r1 = requests.get("http://www.google.com", proxies=proxies)
    r1_time = time.time()
    r2 = requests.get("https://www.stanford.edu/", proxies=proxies) # HTTPS request
    r2_time = time.time()
    r3 = requests.get("https://www.scs.stanford.edu/20sp-cs244b/", proxies=proxies)
    r3_time = time.time()

    # LRU: stanford.edu should be in cache, google should not be request stanford again, then request yahoo. SCS should be booted from cache, not stanford


if __name__ == "__main__":
    run_client()
