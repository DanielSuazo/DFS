Simple DFS (Distributed File System) by Daniel Suazo (based off of template by Prof. Jose Ortiz)

Requirements:
Python 3

Setup:
Create an empty database file by running "python createdb.py".

Step 1:

Run the Meta Data server using "python meta-data.py <port, default=8000>

Step 2:

Run at least one Data Node server using "python data-node <address> <port> <directory_path> <metadata_server_port default=8000>"

Note: All data nodes must have a unique ip/port combination. Also, you must create a directory for every data node server

Step 3:

Now you can use copy.py to copy files to and from your DFS and ls.py to view all files stored in your DFS.

How to use copy.py to copy a file to the DFS:

"python copy.py <filename> <metadata_server_ip:metadata_server_port:filename>"

How to use copy.py to copy a file from the DFS:

"python copy.py <metadata_server_ip:metadata_server_port:filename> <filename>"

How to use ls.py:

"python ls.py <metadata_server_ip> <metadata_server_port default=8000>"

This will return a list of all files in the DFS along with their corresponding file sizes

References used:
Eduardo Santin
Gabriel Santiago
Prof. Jose Ortiz
