import socket
from constants import PKT_SIZE

def run_client():
    host = socket.gethostname()
    port = 5001

    client_socket = socket.socket()
    client_socket.connect((host, port))

    client_socket.send("Hello from client".encode())
    print(client_socket.recv(1024))

if __name__ == "__main__":
    run_client()