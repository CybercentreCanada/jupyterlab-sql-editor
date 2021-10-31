Your new extension project has enough code in it to see it working in your JupyterLab. Run the following commands to install the initial project dependencies and install the extension into the JupyterLab environment.


pip install -ve .


The above command copies the frontend part of the extension into JupyterLab. We can run this pip install command again every time we make a change to copy the change into JupyterLab. Even better, we can use the develop command to create a symbolic link from JupyterLab to our source directory. This means our changes are automatically available in JupyterLab:

jupyter labextension develop --overwrite .




JupyterLab extensions for JupyterLab 3.0 can be distributed as Python packages. The cookiecutter template we used contains all of the Python packaging instructions in the pyproject.toml file to wrap your extension in a Python package. Before generating a package, we first need to install build.

To create a Python wheel package (.whl) in the dist/ directory, do:

python -m build


Both of these commands will build the JavaScript into a bundle in the jupyterlab_apod/labextension/static directory, which is then distributed with the Python package. This bundle will include any necessary JavaScript dependencies as well. You may want to check in the jupyterlab_apod/labextension/static directory to retain a record of what JavaScript is distributed in your package, or you may want to keep this “build artifact” out of your source repository history.

You can now try installing your extension as a user would. Open a new terminal and run the following commands to create a new environment and install your extension.


conda create -n jupyterlab-apod jupyterlab
conda activate jupyterlab-apod
pip install jupyterlab_apod/dist/jupyterlab_apod-0.1.0-py3-none-any.whl
jupyter lab




## Applying LSP code completion to SQL within python strings

jupyterlab-lsp declares transclusions which extract code and send it to the appropriate LSP server. You can see the code extracted by these transclusion since it writes them to virtual documents.

```
# from the workspace root
$ ls -lh .virtual_documents/Untitled4.ipynb.python-1\(sql\).sql
```

We added a transclussion to extract the SQL between the `--start-sql-syntax` and `--end-sql-syntax` markers. This enables code completion within python string of the form
```
sql = '''
	--start-sql-syntax
	SELECT
		*
	FROM
		table1
	--end-sql-syntax markers
	'''
```

### Modifying in place to test custom transclution

```
$ find /usr/ -name 364.560fb07260ff05330d4d.js
/usr/local/lib/python3.9/site-packages/jupyterlab_lsp/labextensions/@krassowski/jupyterlab-lsp/static/364.560fb07260ff05330d4d.js
/usr/local/share/jupyter/labextensions/@krassowski/jupyterlab-lsp/static/364.560fb07260ff05330d4d.js
```

Replaced
`^%%sql(?: (?:(?:(?:.*?)://(?:.*))|${On}|(?:\\w+ << )|(?:\\w+@\\w+)))?\n?((?:.+\n)?(?:[^]*))`

With this
```
{
    language:"sql",
    pattern:`(--start-sql-syntax)( .*?)?\n([^]*?)(--end-sql-syntax)`,
    foreign_capture_groups:[3],
    is_standalone:true,
    file_extension:"sql"
}
```

Make sure to use `is_standalone: true`. Each extracted part is put in a distince virtual document. For some languages it makes sense to place each extracted part into a single virutal document but not for SQL.

You need to start the lab like this for it to pickup those changes
$ jupyter lab --log-level=DEBUG --watch

You are now able to leverage the `sql-language-server` to code complete your SQL query inside a string.

However you'll notice that as your SQL query gets larger the code cell will switch from python syntax hilighting to SQL syntax hilighting.  This is due to the fact that jupyter-lsp has a builtin behaviour to do this. It can be found here.

https://github.com/krassowski/jupyterlab-lsp/blob/a52d4220ab889d0572091410db7f77fa93652f1c/packages/jupyterlab-lsp/src/features/syntax_highlighting.ts#L90

```
		// change the mode if the majority of the code is the foreign code
        if (coverage > this.settings.composite.foreignCodeThreshold) {
          editors_with_current_highlight.add(ce_editor);
          let old_mode = editor.getOption('mode');
          if (old_mode != mode.mime) {
            editor.setOption('mode', mode.mime);
          }
```

Luckily the threshold can be configured in the settings

> Advanced Settings Editor -> Code Syntax -> foreignCodeThreshold: 0.5

We can prevent this behaviour by increasing the threshold.

> Advanced Settings Editor -> Code Syntax -> foreignCodeThreshold: 0.99


You'll now be able to write a code cell with a string which contains lots of SQL while keeping the cell python syntax higlighted. In the next section we will show how to syntax highlight only the SQL statement and keeping the python syntax highlighing for everything else. The best of both worlds.


```
def func2():
    return 2


sql = '''
    --start-sql-syntax

	SELECT
      e.employee_id AS "Employee #"
      , e.first_name || ' ' || e.last_name AS "Name"
      , e.email AS "Email"
      , e.phone_number AS "Phone"
      , TO_CHAR(c., 'MM/DD/YYYY') AS "Hire Date"
    FROM employees e
      JOIN jobs j
        ON e.job_id = j.job_id
      LEFT JOIN employees m
        ON e.manager_id = m.manager_id
      LEFT JOIN departments d
        ON d.department_id = e.department_id
      LEFT JOIN employees dm
        ON d.manager_id = dm.employee_id
      LEFT JOIN locations l
        ON d.location_id = l.location_id
      LEFT JOIN countries c
        ON l.country_id = c.country_id
      LEFT JOIN regions r
        ON c.region_id = r.region_id
      LEFT JOIN job_history jh
        ON e.employee_id = jh.employee_id
      LEFT JOIN jobs jj
        ON jj.job_id = jh.job_id
      LEFT JOIN departments d
        ON dd.department_id = jh.department_id
    WHERE c.country_id >= d
      ORDER BY e.employee_id
    AND e.commision_pct == 'sdflj'
    
    --end-sql-syntax
'''

def func1():
    return 1
```





## Syntax highlighting the SQL which is inside the python string

JupyterLab project declares a CodeMirror for the `ipython` language. This is the CodeMirror used by code cells in notebooks.

The CodeMirror is defined here

https://github.com/jupyterlab/jupyterlab/blob/dbdefed9db9332381fe4104bdf53ec314451951f/packages/codemirror/src/codemirror-ipython.ts

You'll notice that the `ipython` CodeMirror delegates to the `python` CodeMirror, or mode as they are called.

It basically registeres a `ipython` mode which is an instance of the `python` mode with slight configuration changes.

In the CodeMirror project there is also a `SQL` mode.

https://github.com/codemirror/CodeMirror/blob/master/mode/sql/sql.js

It defines modes for `text/x-sql` and many others.

```
 CodeMirror.defineMIME("text/x-sql", {
    name: "sql",
```

When you create a notebook code cell with the `%%sql` magic it is this CodeMirror which syntax highlights the SQL. This is becaause jupyter-lsp registered a transclusion which detects `%%sql` and extracts this code as language `sql`.

https://github.com/krassowski/jupyterlab-lsp/blob/39010530eba400bffc56282709343e9fcf8bc778/packages/jupyterlab-lsp/src/transclusions/ipython-sql/extractors.ts

```
new RegExpForeignCodeExtractor({
      language: 'sql',
      pattern: `^%%sql(?: (?:${SQL_URL_PATTERN}|${COMMAND_PATTERN}|(?:\\w+ << )|(?:\\w+@\\w+)))?\n?((?:.+\n)?(?:[^]*))`,
      foreign_capture_groups: [1],
```

Jupyter-lsp will switch to the foreingh language in this case `SQL` only when the ratio of characters extracted by this transclusion versus the entire amount of characters in the code cell is greater than the threashold specified by

> Advanced Settings Editor -> Code Syntax -> foreignCodeThreshold

Keeping it at 0.99 will prevent it from switching. So there is a balance to strike between preventing it to switch to SQL when it's a python code cell with an SQL string and when it's a `%%sql` magic code cell. Setting it at 0.80 seems to work well.


In order to syntax hilight SQL in a python string we need to be able to "nest" an SQL CodeMirror within the python CodeMirror. Lucily CodeMirror supports `multiplexingMode`

https://codemirror.net/doc/manual.html#overview

There are many examples such as JSP within HTML CodeMirror etc.

To experiment we can modify the copy we have on disk.

The CodeMirror is defined here

https://github.com/jupyterlab/jupyterlab/blob/master/packages/codemirror/src/codemirror-ipython.ts

```
$ find /usr -name *jlab_core*
find: /usr/sbin/authserver: Permission denied
/usr/local/lib/python3.9/site-packages/jupyterlab/static/jlab_core.4cb3c625b5e8bf236b99.js
/usr/local/lib/python3.9/site-packages/jupyterlab/static/jlab_core.4cb3c625b5e8bf236b99.js.LICENSE.txt
/usr/local/share/jupyter/lab/staging/build/jlab_core.f8da8a4bfd6c723cd47f.js
/usr/local/share/jupyter/lab/static/jlab_core.f8da8a4bfd6c723cd47f.js
```

We can modified in place the ipython code mirror to be multiplexed with SQL like so
```
	// instead of returning this mode we multiplex it with SQL
    var pythonMode = codemirror__WEBPACK_IMPORTED_MODULE_0___default().getMode(config, pythonConf);
    // get a mode for SQL
	var sqlMode = codemirror__WEBPACK_IMPORTED_MODULE_0___default().getMode(config, "sql")
	// multiplex python with SQL and return it
    return codemirror__WEBPACK_IMPORTED_MODULE_0___default().multiplexingMode(pythonMode, {
      open:  "--start-sql-syntax",
      close: "--end-sql-syntax",
      parseDelimiters: true,
      mode: sqlMode
    });
```

Now the mode registered for `ipython` is a multiplex mode which can switch to SQL when it sees the `--start-sql-syntax` and `--end-sql-syntax` markers.

The code cell now syntax hightights python and SQL correctly. And thanks to our transclusion it leverages `sql-language-server` for auto-complete support.

```
def func2():
    return 2


sql = '''
    --start-sql-syntax

	SELECT
      e.employee_id AS "Employee #"
      , e.first_name || ' ' || e.last_name AS "Name"
      , e.email AS "Email"
      , e.phone_number AS "Phone"
      , TO_CHAR(c., 'MM/DD/YYYY') AS "Hire Date"
    FROM employees e
      JOIN jobs j
        ON e.job_id = j.job_id
      LEFT JOIN employees m
        ON e.manager_id = m.manager_id
      LEFT JOIN departments d
        ON d.department_id = e.department_id
      LEFT JOIN employees dm
        ON d.manager_id = dm.employee_id
      LEFT JOIN locations l
        ON d.location_id = l.location_id
      LEFT JOIN countries c
        ON l.country_id = c.country_id
      LEFT JOIN regions r
        ON c.region_id = r.region_id
      LEFT JOIN job_history jh
        ON e.employee_id = jh.employee_id
      LEFT JOIN jobs jj
        ON jj.job_id = jh.job_id
      LEFT JOIN departments d
        ON dd.department_id = jh.department_id
    WHERE c.country_id >= d
      ORDER BY e.employee_id
    AND e.commision_pct == 'sdflj'
    
    --end-sql-syntax
'''

def func1():
    return 1
```






## Packaging it into a JupyterLab extension

So far we've been experimenting by replacing the code in-place. Now that we know what we want to do we can package our code into a JupyterLab extension. At startup our extension will register a `ipython` CodeMirror mode and a transclusion to extract sql from python strings.

This tutorial explains how to create an extension. https://jupyterlab.readthedocs.io/en/stable/extension/extension_tutorial.html

After following this tutorial we have a working extension which can contributes an action and a view to jupyterlab.

Now that we understand how to write an extension we can add the code specific to our goal. That is we want to contribute to the jupyterlab-lsp extension `ILSPCodeExtractorsManager` and to the jupyterlab extension `ICodeMirror`

First we add the necessary pacakge depenencdies
```
jlpm add @krassowski/jupyterlab-lsp
jlpm add lsp-ws-connection
jlpm add @jupyterlab/logconsole
jlpm add @jupyterlab/docmanager
jlpm add @types/codemirror
```

Add the following imports to our `index.ts`
```
import { RegExpForeignCodeExtractor, ILSPCodeExtractorsManager } from '@krassowski/jupyterlab-lsp';
import { ICodeMirror } from '@jupyterlab/codemirror'
```

And run the build, and get the following errors:
```
jlpm run build

node_modules/@krassowski/jupyterlab-lsp/lib/tokens.d.ts:7:62 - error TS2307: Cannot find module './_plugin' or its corresponding type declarations.

7 import { LanguageServer2 as LSPLanguageServerSettings } from './_plugin';
```

I had to manually copy some files into the lib folder. I've reported this issue to the jupyterlab-lsp project and they acknowlege there's an issue. https://github.com/krassowski/jupyterlab-lsp/issues/668

```
cp  node_modules/@krassowski/jupyterlab-lsp/src/_* node_modules/@krassowski/jupyterlab-lsp/lib
```

There was also this issue
```
node_modules/@krassowski/jupyterlab-lsp/lib/virtual/document.d.ts:3:31 - error TS2307: Cannot find module 'lsp-ws-connection/src' or its corresponding type declarations.

3 import { IDocumentInfo } from 'lsp-ws-connection/src';
```

To fix this I modified the document.d.ts file, removing the `/src`
```
import { IDocumentInfo } from 'lsp-ws-connection';
```

So now the extension builds okay.

As the tutorial explains you declare the extensions you depend on in your `requires` declaration.
```
  requires: [ICodeMirror, ILSPCodeExtractorsManager, ISettingRegistry],
  activate: (
    app: JupyterFrontEnd,
    codeMirror: ICodeMirror,
    extractors: ILSPCodeExtractorsManager,
    settingRegistry: ISettingRegistry,
```

However I would get errors saying cannot find the requirements even thought jupyterlab-lsp clearly works and registers it's extensions. I figured out the issue. I had to declare that I want to use the tokens from jupyterlab-lsp and not get a copy of them. That's what I gather anyways. I added this to my package.json and not it works.
```
  "jupyterlab": {
 ...
    "sharedPackages": {
      "@krassowski/jupyterlab-lsp": {
        "bundled": false,
        "singleton": true
       }
     }
   },
```
Got the information from this paragraph https://jupyterlab.readthedocs.io/en/stable/extension/extension_dev.html#requiring-a-service


So now I'm able to patch the `ipython` CodeMirror mode to support sql syntax highligthing within python strings and add a `RegExpForeignCodeExtractor` to extract sql from python strings for the LSP auto-complete feature.



