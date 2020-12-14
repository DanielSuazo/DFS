###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and Daniel Suazo
#
# Description:
# 	data node server for the DFS
#

from Packet import Packet

from sys import argv, getsizeof
import socket
import socketserver
from uuid import uuid1
import os.path


def usage():
    exit(
        f"""Usage: python {argv[0]} <server> <port> <data path> <metadata port,default=8000>""")


def register(meta_ip, meta_port, data_ip, data_port):
    """Creates a connection with the metadata server and
        register as data node
    """

    # Establish connection

    # Create socket and connect to meta data server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((meta_ip, meta_port))

    try:
        response = b"NAK"
        # Initialize packet
        sp = Packet()
        while response == b"NAK":
            # Create reg packet and send to meta data server
            sp.BuildRegPacket(data_ip, data_port)
            sock.sendall(sp.getEncodedPacket())

            # Recieve response from meta data server
            response = sock.recv(1024)

            if response == b"DUP":
                # If data node already registered
                print("Duplicate Registration")

            if response == b"NAK":
                # If internal Server Error
                print("Registratation ERROR")

            if response == b"ACK":
                # If data node registered successfully
                print("Registered succesfully")

    finally:
        sock.close()


class DataNodeTCPHandler(socketserver.BaseRequestHandler):

    def handle_put(self, p):
        """Receives a block of data from a copy client, and 
            saves it with an unique ID.  The ID is sent back to the
            copy client.
        """

        # Tell copy client that data node is ready to recieve block
        self.request.send(b"OK")

        # Generates an unique block id.
        blockid = str(uuid1())

        # Open the file for the new data block.
        fd = open(f"{argv[3]}/{blockid}.dat", "wb+")

        # Recieve 16kb max block and write to file
        fd.write(self.request.recv(16384))

        # Close file
        fd.close()

        # Send back blockid
        self.request.sendall(blockid.encode())

    def handle_get(self, p):

        # Get the block id from the packet
        blockid = p.getBlockID()

        # Read the file with the block id data
        # Send it back to the copy client.
        fd = open(f"{argv[3]}/{blockid}.dat", "rb")

        # Send back block bytes
        self.request.send(fd.read())

    def handle(self):

        # Recieve packet
        msg = b""
        while True:
            buffer = self.request.recv(4096)
            msg += buffer
            if getsizeof(buffer) < 4096:
                break

        print(msg, type(msg))

        # Parse message as Packet
        p = Packet()
        p.DecodePacket(msg)

        # Run command in p
        cmd = p.getCommand()
        if cmd == "put":
            self.handle_put(p)

        elif cmd == "get":
            self.handle_get(p)


if __name__ == "__main__":

    META_PORT = 8000
    if len(argv) < 4:
        usage()

    try:
        HOST = argv[1]
        PORT = int(argv[2])
        DATA_PATH = argv[3]

        if len(argv) > 4:
            META_PORT = int(argv[4])

        if not os.path.isdir(DATA_PATH):
            print(f"Error: Data path {DATA_PATH} is not a directory.")
            usage()
    except:
        usage()

    register("localhost", META_PORT, HOST, PORT)
    server = socketserver.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
