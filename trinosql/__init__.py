
import json
from pathlib import Path

from .trinosql import TrinoSql

def load_ipython_extension(ipython):
    ipython.register_magics(TrinoSql)
