import os
import itertools
import requests
import json
from pandas import json_normalize
from os.path import isabs, join, normpath

def global_REQ():
	MAX_REQUEST_COUNT = 3
	return MAX_REQUEST_COUNT
pwd = "/"


def pretty_print(data):
    """Parse json string into pandas dataframe if possible"""
    try:
        data = json.loads(data)
        for d in data:
            if type(data[d]) is list:
                for file in data[d]:
                    if "file_info" in file:
                        file.pop("file_id")
                        file["file_info"][
                            "size"
                        ] = f"{max(0.001, file['file_info']['size'] / 1024 / 1024):.3f} MB"
                    if "nodes" in file:
                        file["nodes"] = len(file["nodes"])
            else:
                if "file_info" in data[d]:
                    data[d]["file_info"][
                        "size"
                    ] = f"{max(0.001, data[d]['file_info']['size'] / 1024 / 1024):.3f} MB"
            try:
                json_data = json_normalize(data[d])
                if not json_data.empty:
                    print(d, ":")
                    print(json_data.to_string())
                else:
                    print(d, ": empty")
            except:
                print(d, ":", data[d])
    except:
        print("Response JSON parse error")

def check_response(
    resp,
    command: str = "Error",
    pretty_print_enabled=False,
    print_content=True,
    verbose=True,
):
    """
    Check that response is ok and print a result to user
    :param print_content: flag that enables content print to console
    :param command: CLI command to check. Used for user prompt.
    :param verbose: flag that enables print to console
    :param resp: response to check
    :param pretty_print_enabled: flag that orders to parse json data from a response into a pretty pandas dataframe
    :return:
    """
    if resp.status_code // 100 == 2:  # status codes 2xx
        if verbose and print_content:
            if pretty_print_enabled:
                pretty_print(resp.content.decode())
            else:
                print(resp.content.decode())
        return True
    else:
        # Error messages with raw response content are not pretty
        if not pretty_print_enabled and verbose:
            print(f"{command}:", resp.content.decode())
        return False


def check_args(
    command: str, args: tuple, required_operands=None, optional_operands=None
):
    """
    Check that number of arguments is correct.
    :param command: CLI command to check. Used for user prompt.
    :param args: received arguments
    :param required_operands: obligatory positional command operands
    :param optional_operands: optional command operands, must go after all required operands
    :type: list
    :return:
    """
    if required_operands is None:
        required_operands = []
    if optional_operands is None:
        optional_operands = []
    if len(required_operands) > 0:
        if len(args) < 2:
            print(f"{command}: missing {required_operands[0]} operand")
            return False
        for i in range(1, len(required_operands)):
            if len(args) < i + 2:
                print(
                    f"{command}: missing {required_operands[i]} operand after '{args[i]}'"
                )
                return False
    # Check if extra operands are present
    expected_count = len(required_operands) + len(optional_operands)
    if len(args) - 1 > expected_count:
        print(
            f"{command}: extra operands are present, expected [{expected_count}] - got [{len(args) - 1}]"
        )
        return False
    return True


def request_datanodes(datanodes, command, method, data=None):
    resp = None
    MAX_REQUEST_COUNT = global_REQ()
    for try_counter in range(MAX_REQUEST_COUNT):
        try:
            for datanode in datanodes:
                node_address = f"{datanode['ip']}:{datanode['port']}"
                if method == "GET":
                    resp = requests.get(join(node_address, command), data=data)
                elif method == "POST":
                    resp = requests.post(join(node_address, command), data=data)
                elif method == "DELETE":
                    resp = requests.delete(join(node_address, command), data=data)
                return resp
        except Exception:
            pass
    print("Error reading from data-nodes")


def os_read_file(path):
    try:
        return open(path, "rb").read()
    except OSError as e:
        print(e)
        return None


def join_path(filename, destination):
    """
    Join destination dir and filename
    :param filename:
    :param destination:
    :return: joined absolute normalized (without loops) path
    """
    return make_abs(join(destination, filename))


def make_abs(path):
    """
    If path is not absolute, join with a pending working directory
    :param path:
    :return: absolute normalized (without loops) path
    """
    if isabs(path):
        return normpath(path)
    else:
        return normpath(join(pwd, path))


def set_pwd(path):
    """Setter for a pending working directory"""
    global pwd
    pwd = path


def get_pwd():
    """Getter for a pending working directory"""
    return pwd
MASTER_NODE = os.getenv("MASTER_NODE", "http://localhost:3030/")


def show_help(*unused):
    # This function defines the commands that are being written from the command line
    print("Commands and Arguments:\n Note: <required_pos_operand>, [optional_operand]\n")
    print("init                 : Initialize the storage")
    print("ping                 : Check the availability of the filesystem")
    print("status               : View the status of the filesystem")
    print("cp <file> <target>  : Copy a file to a specified target path with a new filename")
    print("mv <file> <dest>    : Move a file to a specified destination directory")
    print("put <file> <dest>   : Upload a local file to the remote filesystem")
    print("mkdir <dir>         : Create a specified directory")
    print("cd <dest>           : Change the remote current working directory to the specified destination")
    print("get <file> <dest>   : Download a remote file to the local filesystem")
    print("rm <dest>           : Remove a destination directory or a file")
    print("ls [dest]           : List file information for a destination; no [dest] defaults to '.'")





def ping_master_node(*unused):
    path_to_ping = os.path.join(MASTER_NODE, "ping") # the path becomes: http://localhost:3030/ping 
    resp = requests.get(path_to_ping)
    check_response(resp, "ping")

# check the status of Master Node
def status(*unused):
    path_status_master_node = os.path.join(MASTER_NODE, "status")
    resp = requests.get(path_status_master_node)
    check_response(resp, "status", pretty_print_enabled=True, print_content=True, verbose=True)

# Clear filesystem, prepare it for work
def initialize_filesystem(*unused):
    path_to_delete_fs = os.path.join(MASTER_NODE, "filesystem")
    resp = requests.delete(path_to_delete_fs)
    check_response(resp, "init")
