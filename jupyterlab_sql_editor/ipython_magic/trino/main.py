import json
import logging
import pathlib
import shutil
import subprocess
import sys

from jupyter_lsp.types import LanguageServerManagerAPI

from jupyterlab_sql_editor.ipython_magic.util import find_nvm_lib_dirs

logging.basicConfig(
    format="%(asctime)s %(message)s",
    filename="/tmp/trino-language-server-entrypoint.log",
    level=logging.INFO,
)

mgr = LanguageServerManagerAPI()

# If jupyterlab-lsp has difficulty finding your sql-language-server
# installation, specify additional node_modules paths
mgr.node_roots = ["/usr/local/lib/"]
mgr.node_roots.extend(find_nvm_lib_dirs())

CONFIG = pathlib.Path(__file__).parent.parent
NODE_MODULE = KEY = "sql-language-server"
SCRIPTS = ["dist", "bin", "cli.js"]
PATH_TO_BIN_JS = mgr.find_node_module(NODE_MODULE, *SCRIPTS)

try:
    NODE_LOCATION = shutil.which("node") or shutil.which("node.exe") or shutil.which("node.cmd")
    NODE = str(pathlib.Path(NODE_LOCATION).resolve())
    logging.info(f"Node location: {NODE}")
except Exception:
    NODE_LOCATION = NODE = ""
    logging.error("Failed to find the node binary.")

if PATH_TO_BIN_JS is None:
    logging.error("Failed to find the sql-language-server binary.")


def main():
    if NODE and PATH_TO_BIN_JS:
        logging.info(f"Path to Node: {NODE}. Path to sql-language-server binary: {PATH_TO_BIN_JS}.")
        process = subprocess.Popen([NODE, PATH_TO_BIN_JS, *sys.argv[1:]], stdin=sys.stdin, stdout=sys.stdout)
        logging.info(f"Node process started with pid: {process.pid}")
        sys.exit(process.wait())


def load(app):
    logging.info("trino language server load function called.")
    if not NODE or not PATH_TO_BIN_JS:
        raise Exception("Node and/or sql-language-server binary not available.")
    try:
        config_schema = json.loads((CONFIG / "{}.schema.json".format(KEY)).read_text(encoding="utf-8"))
    except Exception as e:
        logging.error(f"Failed to load config schema: {e}")
        config_schema = {}
    return {
        "trino-language-server": {
            "version": 2,
            "argv": ["trino_language_server", "up", "--method", "stdio"],
            "languages": ["trino"],
            "display_name": "Trino language server",
            "mime_types": ["application/trino", "text/x-trino"],
            "config_schema": config_schema,
        }
    }


if __name__ == "__main__":
    main()
