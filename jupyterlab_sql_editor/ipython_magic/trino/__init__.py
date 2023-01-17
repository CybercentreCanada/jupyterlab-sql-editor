from .trino import Trino


def load_ipython_extension(ipython):
    ipython.register_magics(Trino)
