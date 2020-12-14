###############################################################################
#
# Filename: copy.py
# Author: Jose R. Ortiz and Daniel Suazo
#
# Description:
# 	Copy client for the DFS
#
#

import socket
from os import path, stat

from sys import argv, getsizeof

from Packet import Packet


def usage():
    exit(
        f"""Usage:\n\tFrom DFS: python {argv[0]} <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python {argv[0]} <source file> <server>:<port>:<dfs file path>""")


def copyToDFS(address, fname, path):
    """ Contact the metadata server to ask to copu file fname,
        get a list of data nodes. Open the file in path to read,
        divide in blocks and send to the data nodes.
    """

    # Create a connection to the data server
    metaSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        metaSock.connect(address)
    except:
        exit("Connection to Metadata Server failed, exiting...")

    # Get file size
    fsize = stat(fname).st_size

    # Create a Put packet with the fname and the length of the data,
    # and send it to the metadata server
    p = Packet()
    p.BuildPutPacket(path, fsize)
    metaSock.sendall(p.getEncodedPacket())

    # If no error or file exists
    # Get the list of data nodes.

    message = b""
    while buffer := metaSock.recv(1024):
        message += buffer
    if message and message == b"DUP":
        exit("File already in server, exiting...")
    elif message == b"NAK":
        exit("Server Error, exiting...")
    else:
        p.DecodePacket(message)
        print("Succesfully saved to inode")

    # Close socket
    metaSock.close()

    # Open file and get data node list
    fd = open(fname, "rb")
    dataNodes = p.getDataNodes()

    blockIds = []

    # Send each node its 16kb chunk(s) and recieve each block id
    block = fd.read(16384)
    while block:
        for ip, addr in dataNodes:
            # Create a socket to data node and connect to it
            nodeSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            nodeSock.connect((ip, addr))

            # Send put packet to data node and wait for OK signal
            nodeSock.send(p.getEncodedPacket())

            if nodeSock.recv(1024) == b"OK":
                nodeSock.send(block)

            # Recieve block id from data node and store in blockIds
            bid = nodeSock.recv(1024)

            blockIds.append((ip, addr, str(bid.decode())))

            # Read next 16 kb and exit loop if EOF was already reached
            if not (block := fd.read(16384)):
                break

            # Close socket
            nodeSock.close()

    # Notify the metadata server where the blocks are saved.

    # Build packet
    p.BuildDataBlockPacket(path, blockIds)

    # Create final socket to Metadata server and connect do it
    finalSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        finalSock.connect(address)
    except:
        exit("Connection to Metadata Server failed, exiting...")

    # Send the created packet
    finalSock.sendall(p.getEncodedPacket())


def copyFromDFS(address, fname, path):
    """ Contact the metadata server to ask for the file blocks of
        the file fname.  Get the data blocks from the data nodes.
        Saves the data in path.
    """

    # Contact the metadata server to ask for information of fname

    # Create socket and connect to metadata server
    metaSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    metaSock.connect(address)

    # Create get packet and send request to metadata server
    p = Packet()
    p.BuildGetPacket(fname)

    metaSock.sendall(p.getEncodedPacket())

    # Recieve chunk list
    msg = b""
    while True:
        buffer = metaSock.recv(2048)
        msg += buffer
        if getsizeof(buffer) < 2048:
            break

    # If file not found, exit, if file found, decode packet
    if msg != b"NFOUND":
        p.DecodePacket(msg)
    else:
        exit("File not found, exiting...")

    # If there is no error response Retreive the data blocks

    # Get blocks
    blocks = p.getDataNodes()

    # Open file
    fd = open(path, "wb+")
    for ip, addr, blockId in blocks:
        # Create socket and connect to node
        nodeSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nodeSock.connect((ip, addr))

        # Build packet and send request to data node
        p.BuildGetDataBlockPacket(blockId)
        nodeSock.send(p.getEncodedPacket())

        # Recieve block and write to file named path
        fd.write(nodeSock.recv(16384))

        # Close socket
        nodeSock.close()


if __name__ == "__main__":
    #	client("localhost", 8000)
    if len(argv) < 3:
        usage()

    file_from = argv[1].split(":")
    file_to = argv[2].split(":")

    if len(file_from) > 1:
        ip = file_from[0]
        port = int(file_from[1])
        from_path = file_from[2]
        to_path = argv[2]

        if path.isdir(to_path):
            print(
                f"Error: path {to_path} is a directory.  Please name the file.")
            usage()

        copyFromDFS((ip, port), from_path, to_path)

    elif len(file_to) > 1:
        ip = file_to[0]
        port = int(file_to[1])
        to_path = file_to[2]
        from_path = argv[1]

        if path.isdir(from_path):
            print(
                f"Error: path {from_path} is a directory.  Please name the file.")
            usage()

        copyToDFS((ip, port), from_path, to_path)
