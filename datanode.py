import os
import shutil
import threading
import time

import requests
from flask import request, Response, jsonify

import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler

import requests
from flask import Flask

app = Flask(__name__)
DEBUG = True if os.getenv("DEBUG", "false") == "true" else False
PORT = int(os.getenv("PORT_D", 2020))
FILE_STORE = os.getenv("FILE_STORE", "./data1")
MASTER_NODE = os.getenv("MASTER_NODE", "http://localhost:3030")


def create_log(app, node_name, debug=False):
    if not os.path.exists("./logs"):
        os.mkdir("./logs")
    open(f"./logs/{node_name}.log", "w+")
    file_handler = RotatingFileHandler(
        f"./logs/{node_name}.log", "a", 1 * 1024 * 1024, 10
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)
    if debug:
        app.logger.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info(f"{node_name} startup")


def ping_master():
    import time

    while True:
        try:
            r = requests.post(os.path.join(MASTER_NODE, f"datanode?port={PORT}"))
        except:
            app.logger.error(f"Could not connect to Master Node - {MASTER_NODE}")
        time.sleep(5)


def init_node():
    if not os.path.exists(FILE_STORE):
        os.mkdir(FILE_STORE)
    try:
        requests.post(os.path.join(MASTER_NODE, f"datanode?port={PORT}"))
    except:
        app.logger.error(f"Could not connect to Master Node - {MASTER_NODE}")
        sys.exit(-1)
    ping_thread = threading.Thread(target=ping_master)
    ping_thread.start()
    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)
    ping_thread.join()

@app.route("/ping")
def ping():
    return Response("Data Node is Alive")


@app.errorhandler(Exception)
def handle_exception(e):
    status_code = 400
    if isinstance(e, FileNotFoundError):
        status_code = 404
    app.logger.error(
        "Error during working:" + str(e) + ", request by url:" + str(request.url)
    )
    return Response(str(e), status=status_code)


@app.route("/filesystem", methods=["DELETE", "GET"])
def filesystem():
    if request.method == "DELETE":
        try:
            # remove the storage dir with all its contents and create it anew
            shutil.rmtree(FILE_STORE, ignore_errors=True)
            os.mkdir(FILE_STORE)
            app.logger.info("Storage is cleared")
            return Response(status=200)
        except Exception as e:
            app.logger.info("Error clearing storage")
            return Response(f"Error clearing storage", 400)

    elif request.method == "GET":
        if "files" not in request.json:
            return Response(status=400)
        file_ids = request.json["files"]
        for fid in os.listdir(FILE_STORE):
            if int(fid) not in file_ids:
                os.remove(os.path.join(FILE_STORE, fid))
                app.logger.info(
                    f"Deleting File with fid={fid} as file not found on master"
                )
        app.logger.debug("Sent storage data to a Master Node")
        return jsonify(
            {
                "files": [int(fid) for fid in os.listdir(FILE_STORE)],
                "file_sizes": [
                    os.path.getsize(os.path.join(FILE_STORE, fid))
                    for fid in os.listdir(FILE_STORE)
                ],
            }
        )


@app.route("/file", methods=["GET", "POST", "PUT", "DELETE"])
