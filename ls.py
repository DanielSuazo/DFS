###############################################################################
#
# Filename: ls.py
# Author: Jose R. Ortiz and Daniel Suazo
#
# Description:
# 	List client for the DFS
#

import socket
from sys import argv
from Packet import Packet


def usage():
    print(f"Usage: python {argv[0]} <server>:<port, default=8000>")
    exit(0)


def client(ip, port):
    # Contacts the metadata server and ask for list of files.

    # Create socket and connect to meta data server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((ip, port))
    except:
        exit("Could not connect to Metadata Server. Exiting")

    # Create list packet and encode it
    pack = Packet()
    pack.BuildListPacket()
    encoded_packet = pack.getEncodedPacket()

    # Send list packet
    try:
        sock.send(encoded_packet)
    except Exception as e:
        exit(e)

    # Recieve the response packet as a byte string and decode it into pack
    message = b""
    while buffer := sock.recv(1024):
        message += buffer

    if message != b"NAK":
        try:
            # If packet formatter properly
            pack.DecodePacket(message)
        except Exception as e:
            # If other error
            print(type(message))
            exit(e)
    else:
        # If Internal Server Error
        exit("Error in returned packet")

    # Extract files
    files = pack.getFileArray()

    # Print out files and size
    for name, size in files:
        print(f"{name} {size} bytes")

    # Close socket
    sock.close()


if __name__ == "__main__":

    if len(argv) < 2:
        usage()

    ip = None
    port = None
    server = argv[1].split(":")
    if len(server) == 1:
        ip = server[0]
        port = 8000
    elif len(server) == 2:
        ip = server[0]
        port = int(server[1])

    if not ip:
        usage()

    client(ip, port)
