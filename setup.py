"""
jupyterlab_sql_editor setup
"""
import json
import sys
from pathlib import Path

import setuptools

HERE = Path(__file__).parent.resolve()

# The name of the project
name = "jupyterlab_sql_editor"

lab_path = HERE / name.replace("-", "_") / "labextension"

# Representative files that should exist after a successful build
ensured_targets = [str(lab_path / "package.json"), str(lab_path / "static/style.js")]

labext_name = "jupyterlab-sql-editor"

data_files_spec = [
    ("share/jupyter/labextensions/%s" % labext_name, str(lab_path.relative_to(HERE)), "**"),
    ("share/jupyter/labextensions/%s" % labext_name, str("."), "install.json"),
    ("share/jupyter/lab/settings/overrides.d", str("overrides"), "trino-lsp.json"),
    ("share/jupyter/lab/settings/overrides.d", str("overrides"), "sparksql-lsp.json"),
    ("share/jupyter/lab/settings/overrides.d", str("overrides"), "syntax_highlighting.json"),
    ("jupyterlab_sql_editor/ipython_magic/sparksql/", str("jupyterlab_sql_editor/ipython_magic/sparksql/"), "*.gif"),
    ("jupyterlab_sql_editor/ipython_magic/", str("jupyterlab_sql_editor/ipython_magic/"), "*.json"),
]

long_description = (HERE / "README.md").read_text()

# Get the package info from package.json
pkg_json = json.loads((HERE / "package.json").read_bytes())

setup_args = dict(
    name=name,
    version=pkg_json["version"],
    url=pkg_json["homepage"],
    author=pkg_json["author"]["name"],
    author_email=pkg_json["author"]["email"],
    description=pkg_json["description"],
    license=pkg_json["license"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=[
        "cccs-ipyaggrid>=0.5.3",
        "ipydatagrid",
        "ipytree",
        "Jinja2",
        "jupyter-events>=0.6.1",
        "pandas>=1.4.4",
        "ply",
        "pyspark",
        "sqlparse",
        "trino",
        "mkdocs",
    ],
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.6",
    platforms="Linux, Mac OS X, Windows",
    keywords=[
        "sql",
        "Jupyter",
        "JupyterLab",
        "JupyterLab3",
        "jupyter",
        "jupyterlab-extension",
        "spark",
        "trino",
        "dataframe",
        "cccs",
        "canada",
    ],
    classifiers=[
        "Framework :: Jupyter",
        "Framework :: Jupyter :: JupyterLab",
        "Framework :: Jupyter :: JupyterLab :: 3",
        "Framework :: Jupyter :: JupyterLab :: Extensions",
        "Framework :: Jupyter :: JupyterLab :: Extensions :: Prebuilt",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "console_scripts": [
            "sparksql_language_server = jupyterlab_sql_editor.ipython_magic.sparksql.main:main",
            "trino_language_server = jupyterlab_sql_editor.ipython_magic.trino.main:main",
        ],
        "jupyter_lsp_spec_v1": [
            "trino-language-server = jupyterlab_sql_editor.ipython_magic.trino.main:load",
            "sparksql-language-server = jupyterlab_sql_editor.ipython_magic.sparksql.main:load",
        ],
    },
)

try:
    from jupyter_packaging import get_data_files, npm_builder, wrap_installers

    post_develop = npm_builder(build_cmd="install:extension", source_dir="src", build_dir=lab_path)
    setup_args["cmdclass"] = wrap_installers(post_develop=post_develop, ensured_targets=ensured_targets)
    setup_args["data_files"] = get_data_files(data_files_spec)
except ImportError as e:
    import logging

    logging.basicConfig(format="%(levelname)s: %(message)s")
    logging.warning("Build tool `jupyter-packaging` is missing. Install it with pip or conda.")
    if not ("--name" in sys.argv or "--version" in sys.argv):
        raise e

if __name__ == "__main__":
    setuptools.setup(**setup_args)
