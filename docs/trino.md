# trino magic

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

## Usage

Parameter usage example:

```
%%trino -c catalog -l 10 --dataframe df
<QUERY>
```

| Parameter                                                                                                                        | Description                                                                                                                                                                                                 |
| -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-c NAME`, `--catalog NAME`                                                                                                      | Trino catalog to use.                                                                                                                                                                                       |
| `-s NAME`, `--schema NAME`                                                                                                       | Trino schema to use.                                                                                                                                                                                        |
| `-l LIMIT`, `--limit LIMIT`                                                                                                      | The maximum number of rows to display. A value of zero is equivalent to `--output skip`                                                                                                                     |
| `-r all\|none`, `--refresh all\|none`                                                                                            | Force the regeneration of the schema cache file.                                                                                                                                                            |
| `-d NAME`, `--dataframe NAME`                                                                                                    | Capture results in pandas dataframe named `NAME`.                                                                                                                                                           |
| `-i NAME`, `--input NAME`                                                                                                        | Display pandas dataframe named `NAME`.                                                                                                                                                                      |
| `-o sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none`, `--output sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` | Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements).                                                               |
| `-s`, `--show-nonprinting`                                                                                                       | Replace none printable characters with their ascii codes (`LF` -> `\x0a`).                                                                                                                                  |
| `-j`, `--jinja`                                                                                                                  | Enable Jinja templating support.                                                                                                                                                                            |
| `-t LIMIT`, `--truncate LIMIT`                                                                                                   | Truncate output.                                                                                                                                                                                            |
| _DEPRECATED_ ~~`-x STATEMENT`, `--raw STATEMENT`~~                                                                               | ~~Run statement as is. Do not wrap statement with a limit. Use this option to run statement which can't be wrapped in a `SELECT`/`LIMIT` statement. For example `EXPLAIN`, `SHOW TABLE`, `SHOW CATALOGS`.~~ |
