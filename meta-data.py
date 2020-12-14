###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and Daniel Suazo
#
# Description:
# 	MySQL support library for the DFS project. Database info for the
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import mds_db
from Packet import Packet
from sys import argv, getsizeof
import socketserver


def usage():
    print(f"Usage: python {argv[0]} <port, default=8000>")
    exit(0)


class MetadataTCPHandler(socketserver.BaseRequestHandler):

    def handle_reg(self, db, p):
        """Register a new client to the DFS "ACK" if successfully REGISTERED
            "NAK" if problem, "DUP" if the IP and port already registered
        """

        # db.AddDataNode() returns nid if the node is added, returns 0 if it was a duplicate,
        # and raises an exeption on any other error
        try:
            if db.AddDataNode(p.getAddr(), p.getPort()):
                self.request.sendall(b"ACK")
            else:
                self.request.sendall(b"DUP")
        except:
            self.request.sendall(b"NAK")

    def handle_list(self, db):
        """Get the file list from the database and send list to client"""
        try:
            # Create packet
            p = Packet()

            # Get list of tuples (fname, fsize) and insert into packet
            file_list = db.GetFiles()
            p.BuildListResponse(file_list)

            # Send the encoded packet
            self.request.send(p.getEncodedPacket())
        except:
            self.request.sendall(b"NAK")

    def handle_put(self, db, p):
        """Insert new file into the database and send data nodes to save
            the file.
        """

        # Get file info
        info = p.getFileInfo()
        # Get list of active data nodes and send them to cop client
        if nodes := db.GetDataNodes():
            if db.InsertFile(info[0], info[1]):
                # If file was inserted properly, send back list of data nodes
                p.BuildPutResponse(nodes)
                self.request.sendall(p.getEncodedPacket())
            else:
                # If file was already in server, return Duplicate Error
                self.request.sendall(b"DUP")
        else:
            # If no data nodes have registered yet, return Server Error
            self.request.sendall(b"NAK")

    def handle_get(self, db, p):
        """Check if file is in database and return list of
                server nodes that contain the file.
        """

        # Get filesize and chuck list from database
        fsize, chunks = db.GetFileInode(p.getFileName())

        if fsize:
            # If file is in database, send back the chunk list
            p.BuildGetResponse(chunks, fsize)
            self.request.sendall(p.getEncodedPacket())
        else:
            # If file not in database, return NOT FOUND
            self.request.sendall(b"NFOUND")

    def handle_blocks(self, db, p):
        """Add the data blocks to the file inode"""

        # Add fname, fsize, to database table inode
        db.AddBlockToInode(p.getFileName(), p.getDataBlocks())

    def handle(self):

        # Establish a connection with the local database
        db = mds_db("dfs.db")
        db.Connect()

        # Define a packet object to decode packet messages
        p = Packet()

        # Receive a msg from the list, data-node, or copy clients
        msg = b""
        while True:
            buffer = self.request.recv(2048)
            msg += buffer
            if getsizeof(buffer) < 2048:
                break
        print(msg, type(msg))

        # Decode the packet received
        try:
            p.DecodePacket(msg)
        except:
            print("Error in recieved packet")

        # Extract the command part of the received packet
        cmd = p.getCommand()

        # Invoke the proper action
        if cmd == "reg":
            # Registration client
            self.handle_reg(db, p)

        elif cmd == "list":
            self.handle_list(db)

        elif cmd == "put":
            # Client asking for servers to put data
            # Fill code
            self.handle_put(db, p)

        elif cmd == "get":
            # Client asking for servers to get data
            # Fill code
            self.handle_get(db, p)

        elif cmd == "dblks":
            # Client sending data blocks for file
            # Fill code
            self.handle_blocks(db, p)

        db.Close()


if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(argv) > 1:
        try:
            PORT = int(argv[1])
        except:
            usage()

    server = socketserver.TCPServer((HOST, PORT), MetadataTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
