# jupyterlab-sql-editor

A JupyterLab extension providing the following features via `%%sparksql` and `%%trino` magics:

- SQL formatter
- Automatic extraction of database schemas
- Auto-completion triggered by `tab` or `dot` for:
  - table names
  - table aliases
  - table joins
  - nested column names
  - functions
- Syntax highlighting for:
  - line magic
  - cell magic
  - Python strings
