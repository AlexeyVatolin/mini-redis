import socket
from threading import Thread


def on_new_client(connection: socket.socket, addr: tuple[str, int]) -> None:
    with connection:
        print(f"Connected by {addr}")
        while True:
            data = connection.recv(1024)
            if not data:
                break
            connection.sendall(b"+PONG\r\n")
        connection.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(5)
    while True:
        connection, addr = server_socket.accept()
        thread = Thread(target=on_new_client, args=(connection, addr))
        thread.start()


if __name__ == "__main__":
    main()
