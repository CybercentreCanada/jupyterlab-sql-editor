
import json
from pathlib import Path

from .trinosql import Trino

def load_ipython_extension(ipython):
    ipython.register_magics(Trino)
