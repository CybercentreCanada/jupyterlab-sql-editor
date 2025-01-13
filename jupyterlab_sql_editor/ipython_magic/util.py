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


def merge_schemas(original, incoming):
    """
    Deep merge two dictionaries. Modifies original.
    For key conflicts if both values are:
     a. dict: Recursively call merge_schemas on both values.
     b. list: Concatenates the two lists and dedup items to avoid repeated entries.
     c. any other type: Value is overridden.
    """
    for key in incoming:
        if key in original:
            if isinstance(original[key], dict) and isinstance(incoming[key], dict):
                merge_schemas(original[key], incoming[key])
            elif isinstance(original[key], list) and isinstance(incoming[key], list):
                original[key] = original[key] + [item for item in incoming[key] if item not in original[key]]
            else:
                original[key] = incoming[key]
        else:
            original[key] = incoming[key]
