# Overview

- [Installation](#installation)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Contributing](#contributing)
- [Set up Trino Server for Testing](#set-up-trino-server-for-testing)

# Installation

## Requirements

- JupyterLab >= 3.0

## Install

To install the extension, execute:

```bash
pip install jupyterlab-lsp jupyterlab-sql-editor trino pyspark
```

## Uninstall

To remove the extension, execute:

```bash
pip uninstall jupyterlab-sql-editor
```

# Configure auto completion

Auto completion leverages the sql-language-server project.

### Install sql-language-server

```bash
sudo npm install -g sql-language-server
```

```bash
$ npm list -g
/usr/local/lib
├── n@7.0.1
├── npm@7.11.2
├── sql-language-server@1.1.0
├── yarn@1.22.10
└── yo@3.1.1
```

```
ls -lh /usr/local/lib/node_modules/sql-language-server/
```

Remember the location where your module is installed. We will configure jupyterlab-lsp with this location. In this example, the location is: `/usr/local/lib/`.

# Configuration

## Configure JupyterLab LSP to use registered sql-language-server

You can configure jupyterlab-lsp using the Advanced Settings Editor.

### Using the Advanced Settings Editor

![display](images/jupyterlab-lsp-config.png)

## Pre-configure Magics (optional)

SparkSql and Trino magic can be configured inside a Notebook. However, it's convenient to pre-configure them using an IPython profile.

```bash
$ cat ~/.ipython/profile_default/ipython_config.py

# get the config
c = get_config()

# pre-load the sparksql magic
c.InteractiveShellApp.extensions = [
    'jupyterlab_sql_editor.ipython_magic.trino', 'jupyterlab_sql_editor.ipython_magic.sparksql'
]

# pre-configure the SparkSql magic.
c.SparkSql.limit=20
c.SparkSql.cacheTTL=3600
c.SparkSql.outputFile='/tmp/sparkdb.schema.json'
c.SparkSql.catalogs='default'

# pre-configure the Trino magic.
import trino
c.Trino.auth=trino.auth.BasicAuthentication("principal id", "password")
c.Trino.user=None
c.Trino.host='localhost'
c.Trino.port=443
c.Trino.httpScheme='https'
c.Trino.cacheTTL=3600
c.Trino.outputFile='/tmp/trinodb.schema.json'
c.Trino.catalogs="system,tpch"

# pre-configure to display all cell outputs in notebook
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = 'all'

```

## Configure AG Grid's license key

You can configure AG Grid (the widget that shows up when you use --output aggrid) to use your license key by setting the AG_GRID_LICENSE_KEY environment variable. It'll be accessed through Python's os.environ.get.

# How it works

### Code completion

Code completion is performed by an LSP server [sql-language-server](https://github.com/joe-re/sql-language-server) which plugs into the [jupyterlab-lsp](https://github.com/krassowski/jupyterlab-lsp) JupyterLab extension.

### Transclusions

`jupyterlab-sql-editor` includes transclusions to extract SQL statements from:

- sparksql cell magic
- sparksql line magic
- SQL in a Python strings (using special --start/end-sql-syntax markers)

Transclusions are what [jupyterlab-lsp](https://github.com/krassowski/jupyterlab-lsp) uses to extract parts of a cell (in this case, SQL statements) and pass them to the `sql-language-server` for evaluation. This enables auto completion of spark SQL keywords, tables, columns and functions.

### Syntax Highlighting

`jupyterlab-sql-editor` registers 3 multiplexed CodeMirrors to support precise syntax highlighting. A multiplexed CodeMirror is registered for:

- sparksql cell magic
- sparksql line magic
- SQL in Python strings

Multiplexed CodeMirror is better at detecting what to syntax-highlight as Python and what to syntax-highlight as SQL. It does not rely on `jupyterlab-lsp` heuristic detection which, when passed a given [foreignCodeThreshold](https://github.com/krassowski/jupyterlab-lsp/blob/master/packages/jupyterlab-lsp/schema/syntax_highlighting.json), will change the mode of the entire cell's editor. Instead, multiplexed CodeMirror is able to support SQL mode which is embedded in a Python mode and will apply syntax highlighting to each section of the cell accordingly.

However, you'll notice that as your SQL query gets larger the code cell will switch from Python syntax highlighting to SQL syntax highlighting. This is due to the fact that jupyter-lsp has a builtin behaviour to do this. It can be found here.

https://github.com/krassowski/jupyterlab-lsp/blob/a52d4220ab889d0572091410db7f77fa93652f1c/packages/jupyterlab-lsp/src/features/syntax_highlighting.ts#L90

```
  // change the mode if the majority of the code is the foreign code
  if (coverage > this.settings.composite.foreignCodeThreshold) {
    editors_with_current_highlight.add(ce_editor);
    let old_mode = editor.getOption('mode');
    if (old_mode != mode.mime) {
      editor.setOption('mode', mode.mime);
    }
  }
```

This is why we recommend increasing the `foreignCodeThreshold` to 99%.

# Contributing

We :heart: contributions.

Have you had a good experience with this project? Why not share some love and contribute code, or just let us know about any issues you had with it?

We welcome issue reports [here](../../issues); be sure to choose the proper issue template for your issue, so that we can be sure you're providing the necessary information.

### Development install

Note: You will need NodeJS to build the extension package.

The `jlpm` command is JupyterLab's pinned version of
[yarn](https://yarnpkg.com/) that is installed with JupyterLab. You may use
`yarn` or `npm` in lieu of `jlpm` below.

Clone the repo to your local environment

Run the following commands to install the initial project dependencies and install the extension into the JupyterLab environment.

```bash
pip install -ve .
```

The above command copies the frontend part of the extension into JupyterLab. We can run this pip install command again every time we make a change to copy the change into JupyterLab. Even better, we can use the develop command (shown below) to create a symbolic link from JupyterLab to our source directory. This means our changes are automatically available in JupyterLab!

```bash
jupyter labextension develop . --overwrite
# Rebuild extension Typescript source after making changes
jlpm run build
```

You can watch the source directory and run JupyterLab at the same time in different terminals to watch for changes in the extension's source and automatically rebuild the extension.

```bash
# Watch the source directory in one terminal, automatically rebuilding when needed
jlpm run watch
# Run JupyterLab in another terminal
jupyter lab
```

With the watch command running, every saved change will immediately be built locally and made available in your running JupyterLab. Refresh JupyterLab to load the change in your browser (you may need to wait several seconds for the extension to be rebuilt).

By default, the `jlpm run build` command generates the source maps for this extension to make it easier to debug using the browser dev tools. To also generate source maps for the JupyterLab core extensions, you can run the following command:

```bash
jupyter lab build --minimize=False
```

### Development uninstall

```bash
pip uninstall jupyterlab-sql-editor
```

In development mode, you will also need to remove the symlink created by `jupyter labextension develop`
command. To find its location, you can run `jupyter labextension list` to figure out where the `labextensions`
folder is located. Then you can remove the symlink named `jupyterlab-sql-editor` within that folder.

### Packaging the extension

See [RELEASE](RELEASE.md)

### Installing sql-language-server from source

For development you can install the sql-language-server from source.

```bash
cd pacakges/server
npm run prepublish
```

Link the location of node_modules to your build location. By running JupyterLab with debug output you can see where it will search for node_modules. For example, here's a log when using the bash magic.

```bash
jupyter lab --log-level=DEBUG > jupyter.log 2>&1

cat jupyter.log
[D 2021-09-08 21:10:53.111 ServerApp] Checking for /home/jovyan/node_modules/bash-language-server/bin/main.js
[D 2021-09-08 21:10:53.111 ServerApp] Checking for /opt/conda/share/jupyter/lab/staging/node_modules/bash-language-server/bin/main.js
[D 2021-09-08 21:10:53.111 ServerApp] Checking for /opt/conda/lib/node_modules/bash-language-server/bin/main.js
[D 2021-09-08 21:10:53.111 ServerApp] Checking for /opt/conda/node_modules/bash-language-server/bin/main.js
```

Once you know where JupyterLab looks for node_modules you can create a link to your built version.

```bash
cd /home/jovyan/node_modules/
ln -s ~/dev/sql-language-server/packages/server/ sql-language-server
```

# Set up Trino Server for Testing

Download server

```bash
wget https://repo1.maven.org/maven2/io/trino/trino-server/364/trino-server-364.tar.gz
tar -zxvf trino-server-364.tar.gz
cd trino-server-364
mkdir etc
```

Create a file etc/node.properties

```properties
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/var/trino/data
```

Create a file etc/jvm.config

```properties
-server
-Xmx16G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
```

Create a file etc/config.properties

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=5GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB
discovery.uri=http://localhost:8080
```

Configure the sample TPCH database

```bash
mkdir etc/catalog
```

Create a file etc/catalog/tpch.properties with this content

```properties
connector.name=tpch
```

Launch the Trino server

```bash
bin/launcher start
```
