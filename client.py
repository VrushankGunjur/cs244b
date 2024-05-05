import socket
import requests 
from constants import PKT_SIZE
# Client code makes a request for a webpage, rerouting through server (proxy)

proxies = {
    "http": "http://localhost:5001",
    "https": "http://localhost:5001"
}

def run_client():
    r = requests.get("http://www.google.com", proxies=proxies)
    print(r.text)


    # host = socket.gethostname()
    # port = 5001

    # client_socket = socket.socket()
    # client_socket.connect((host, port))

    # client_socket.send("Hello from client".encode())
    # print(client_socket.recv(1024))

if __name__ == "__main__":
    run_client()