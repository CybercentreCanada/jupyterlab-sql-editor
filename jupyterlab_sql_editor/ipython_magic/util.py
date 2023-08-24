from os import environ, listdir
from os.path import isdir, join


def find_nvm_lib_dirs():
    NVM_VERSIONS_SUBPATH = "/versions/node/"
    nvm_dir = environ["NVM_DIR"]
    dirs = []
    if nvm_dir:
        dirs = [
            f"{nvm_dir}{NVM_VERSIONS_SUBPATH}{d}/lib"
            for d in listdir(nvm_dir + NVM_VERSIONS_SUBPATH)
            if isdir(join(nvm_dir + NVM_VERSIONS_SUBPATH, d))
        ]
    return dirs
