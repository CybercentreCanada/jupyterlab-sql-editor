import logging
import pathlib
import shutil
import subprocess
import sys

from jupyter_lsp.specs.config import load_config_schema
from jupyter_lsp.types import LanguageServerManagerAPI

from jupyterlab_sql_editor.ipython.common import find_nvm_lib_dirs

logging.basicConfig(
    format="%(asctime)s %(message)s",
    filename="/tmp/trino-language-server-entrypoint.log",
    level=logging.INFO,
)


NODE_LOCATION = shutil.which("node") or shutil.which("node.exe") or shutil.which("node.cmd")
NODE = str(pathlib.Path(NODE_LOCATION).resolve())


mgr = LanguageServerManagerAPI()

# If jupyterlab-lsp has difficulty finding your sql-language-server
# installation, specify additional node_modules paths
mgr.node_roots = ["/usr/local/lib/"]
mgr.node_roots.extend(find_nvm_lib_dirs())

NODE_MODULE = KEY = "sql-language-server"
SCRIPTS = ["dist", "bin", "cli.js"]
PATH_TO_BIN_JS = mgr.find_node_module(NODE_MODULE, *SCRIPTS)


def main():
    logging.info("main function called")
    logging.info(f"node location: {NODE}")
    logging.info(f"path to script: {PATH_TO_BIN_JS}")
    process = subprocess.Popen([NODE, PATH_TO_BIN_JS, *sys.argv[1:]], stdin=sys.stdin, stdout=sys.stdout)
    logging.info(f"node process started with pid: {process.pid}")
    sys.exit(process.wait())


def load(app):
    logging.info("load function called")
    return {
        "trino-language-server": {
            "version": 2,
            "argv": ["trino_language_server", "up", "--method", "stdio"],
            "languages": ["trino"],
            "display_name": "Trino language server",
            "mime_types": ["application/trino", "application/x-trino"],
            "config_schema": load_config_schema(KEY),
        }
    }


if __name__ == "__main__":
    main()
