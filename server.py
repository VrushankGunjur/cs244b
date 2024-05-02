import socket
from constants import PKT_SIZE

def run_server():
    host = socket.gethostname()
    port = 5001

    server_socket = socket.socket()
    server_socket.bind((host, port))

    server_socket.listen(5) # how many clients the server can listen to at the same time

    client_socket, addr = server_socket.accept()
    print("Connection from: " + str(addr))
    client_socket.recv(PKT_SIZE)
    client_socket.send("Hello from server".encode())


if __name__ == "__main__":
    run_server()