### YDFS
YDFS is a distributed file system written in Python, designed to offer scalable and fault-tolerant storage solutions for large datasets. It provides high availability, reliability, and efficient data access across a distributed network.

### Features
1.Scalable Architecture: Easily scale storage and compute resources as needed.
2.Fault Tolerance: Handles node failures and ensures data availability.
3.Efficient Data Access: Optimized for high-throughput data access and low-latency operations.
4.Replication: Data is replicated across multiple nodes to ensure redundancy.
5.Metadata Management: Efficiently manages file metadata and directory structures.
# Detailed Explanation of DFS Files

## 1. master_node.py


### Imports and Setup
- Uses Flask for the web server
- Imports various Python libraries for threading, logging, and file operations
- Sets up configuration variables like DEBUG, PORT, and REPLICATION_FACTOR

### FileSystem Class
- Represents the in-memory file system structure
- Manages files and directories using dictionaries
- Implements methods for file and directory operations (add, get, move, remove)

### Flask Routes
1. `/ping`: Simple health check endpoint
2. `/status`: Returns the current status of the file system
3. `/datanode`: Handles Data Node registration
4. `/filesystem`: Manages initialization and querying of the file system
5. `/file`: Handles file operations (GET, POST, PUT, DELETE)
6. `/directory`: Manages directory operations (GET, POST, DELETE)

### Helper Functions
- `request_datanode`: Sends requests to Data Nodes
- `drop_datanode`: Removes a non-responsive Data Node
- `choose_datanodes`: Selects Data Nodes for operations based on load factor
- `choose_datanodes_for_replication`: Selects nodes for file replication

### Background Tasks
- `ping_data_nodes`: Periodically checks Data Node health
- `replication_check`: Ensures proper file replication across Data Nodes

### Main Execution
- Sets up logging
- Starts background threads for Data Node health checks and replication
- Runs the Flask server

## 2. datanode.py

This file implements the Data Node functionality:

### Imports and Setup
- Uses Flask for the web server
- Sets up configuration variables like DEBUG, PORT, FILE_STORE, and MASTER_NODE

### Flask Routes
1. `/ping`: Health check endpoint
2. `/filesystem`: Manages local file system operations
3. `/file`: Handles file operations (GET, POST, PUT, DELETE)

### Helper Functions
- `create_log`: Sets up logging for the Data Node
- `ping_master`: Periodically pings the Master Node to report status

### Main Execution
- Initializes the Data Node (creates storage directory, registers with Master Node)
- Starts a background thread to ping the Master Node
- Runs the Flask server

## 3. file_system.py

This file defines the core data structures for the file system:

### DataNode Class
- Represents a Data Node with IP and port

### File Class
- Represents a file in the system with name, ID, nodes, and file info

### FileSystem Class
- Implements the in-memory file system structure
- Manages files and directories
- Provides methods for file and directory operations

## 4. combined_client.py

This file implements the client interface for interacting with the DFS:

### Imports and Setup
- Uses requests library for HTTP communications
- Sets up configuration like MASTER_NODE address

### Helper Functions
- `pretty_print`: Formats and prints JSON responses
- `check_response`: Validates and processes server responses
- `check_args`: Validates command arguments
- `request_datanodes`: Sends requests to Data Nodes
- `os_read_file`: Reads local files
- `join_path`, `make_abs`: Handle path operations

### Command Functions
Implement various DFS operations:
- `show_help`: Displays available commands
- `ping_master_node`: Checks Master Node availability
- `status`: Retrieves DFS status
- `initialize_filesystem`: Initializes the DFS
- `move_file`: Moves a file
- `copy_file`: Copies a file
- `put_file`: Uploads a local file to DFS
- `change_dir`: Changes current directory
- `make_dir`: Creates a new directory
- `read_file`: Downloads a file from DFS
- `remove_file_or_dir`: Removes a file or directory
- `list_dir`: Lists directory contents

### Main Execution Loop
- Continuously prompts for user commands
- Parses and executes the appropriate command function

This client provides a command-line interface for users to interact with the distributed file system, translating user commands into API calls to the Master Node and Data Nodes as needed.
Step 1: Start the Master Node

Open a terminal window
Navigate to the directory containing your project files
Start the master node by running:
Copypython master_node.py


Step 2: Start Data Nodes
You need to start at least one data node, but you can start multiple for redundancy.

Open a new terminal window for each data node
Navigate to the project directory
Start each data node by running:
Copypython datanode.py
Note: The code already sets up default values for environment variables, so you don't need to set them manually unless you want to override the defaults.

Step 3: Start the Client

Open another terminal window
Navigate to the project directory
Start the client by running:
Copypython combined_client.py


Step 4: Initialize the File System

In the client terminal, type the following command:
Copyinit
This will initialize the distributed file system.
You can now use various commands like ls, mkdir, put, get, etc., to interact with your DFS.
init
status
mkdir /test_dir
cd /test_dir
put local_file.txt remote_file.txt
ls
get remote_file.txt downloaded_file.txt
cp remote_file.txt copy_file.txt
ls
mv copy_file.txt /moved_file.txt
ls
ls /
rm remote_file.txt
ls
cd /
rm /test_dir
status
