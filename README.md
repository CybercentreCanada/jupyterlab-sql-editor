# jupyterlab-sql-editor

A JupyterLab extension providing:

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

IPython magic for:
- Spark SQL
- Trino

Une extension à JupyterLab qui contribue:

- Formattage de SQL
- Extraction automatique du schéma de base de données
- Complétion automatique suivant un `tab` ou `dot` des:
    - noms de tables
    - alias de tables
    - jointures de tables
    - noms de colones imbriquées
    - fonctions
-  Surbrillance de la syntaxe:
    - ligne magic
    - cellule magic
    - chaînes de caractères Python

IPython magic pour:
- Spark SQL
- Trino


## Execute and output your query results into an interactive data grid / Execution et affichage des résultats dans une grille interactive
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//ipydatagrid.gif)

## Output as JSON / Résultats en JSON
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//json-output.gif)

## Auto suggest column names and sub-fields / Suggestion automatique des noms de colones et sous champs
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//sparksql-nested-columns.gif)

## Auto suggest JOINs on matching column names / Suggestion automatique de jointures sur nom de colones
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//spark-inner-join.gif)

## Format and show syntax highlighting in Notebook code cells / Formattage et surbrillance de la syntaxe des cellules
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images/format-cell.gif)

## Works in Python strings / Fonctionne à l'intérieur d'une chaînes de caractères Python
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//python-string.gif)


## Capture your Spark query as a Dataframe or a temporary view / Capture de la requête dans un Dataframe ou vue temporaire
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//args.png)

## Use jinja templating to create re-usable SQL / Utilisation de Jinja pour création de requêtes SQL réutilisables
![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//jinja.png)



## Installation

Follow the installation instructions in [CONTRIBUTING](./CONTRIBUTING.md)

Suivre les instructions d'installation dans [CONTRIBUTING](./CONTRIBUTING.md)

## Parameters / Paramètres
Parameter usage example:
```
%%sparksql -c -l 10 --dataframe df
<QUERY>
```

### sparksql
|Parameter|Description|
|---|---|
|`--database NAME`|Spark database to use.|
|`-l LIMIT` `--limit LIMIT`|The maximum number of rows to display. A value of zero is equivalent to `--output skip`|
|`-r all\|local\|none` `--refresh all\|local\|none`|Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.|
|`-d NAME` `--dataframe NAME`|Capture dataframe in a local variable named `NAME`.|
|`-c` `--cache`|Cache dataframe.|
|`-e` `--eager`|Cache dataframe with eager load.|
|`-v VIEW` `--view VIEW`|Create or replace a temporary view named `VIEW`.|
|`-o sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` `--output sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none`|Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements).|
|`-s` `--show-nonprinting`|Replace none printable characters with their ascii codes (`LF` -> `\x0a`)|
|`-j` `--jinja`|Enable Jinja templating support.|
|`-b` `--dbt`|Enable DBT templating support.|
|`-t LIMIT` `--truncate LIMIT`|Truncate output.|
|`-m update\|complete` `--streaming_mode update\|complete`|The mode of streaming queries.|
|`-x` `--lean-exceptions`|Shortened exceptions. Might be helpful if the exceptions reported by Spark are noisy such as with big SQL queries.|

### trino
|Parameter|Description|
|---|---|
|`-c NAME` `--catalog NAME`|Trino catalog to use.|
|`-s NAME` `--schema NAME`|Trino schema to use.|
|`-l LIMIT` `--limit LIMIT`|The maximum number of rows to display. A value of zero is equivalent to `--output skip`|
|`-r all\|none` `--refresh all\|none`|Force the regeneration of the schema cache file.|
|`-d NAME` `--dataframe NAME`|Capture dataframe in a local variable named `NAME`.|
|`-o sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` `--output sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none`|Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements).|
|`-s` `--show-nonprinting`|Replace none printable characters with their ascii codes (`LF` -> `\x0a`).|
|`-j` `--jinja`|Enable Jinja templating support.|
|`-t LIMIT` `--truncate LIMIT`|Truncate output.|
|`-x STATEMENT` `--raw STATEMENT`|Run statement as is. Do not wrap statement with a limit. Use this option to run statement which can't be wrapped in a SELECT/LIMIT statement. For example EXPLAIN, SHOW TABLE, SHOW CATALOGS.|

## Security Vulnerability Reporting

If you believe you have identified a security vulnerability in this project, please send an email to the project
team at contact@cyber.gc.ca, detailing the suspected issue and any methods you've found to reproduce it.

Please do NOT open an issue in the GitHub repository, as we'd prefer to keep vulnerability reports private until
we've had an opportunity to review and address them.

## Rapports sur les vulnérabilités de sécurité

Si vous pensez avoir identifié une faille de sécurité dans ce projet, veuillez envoyer un courriel à l'équipe du projet à contact@cyber.gc.ca, en détaillant le problème soupçonné et les méthodes que vous avez trouvées pour le reproduire.

Veuillez NE PAS ouvrir un problème dans le GitHub repo, car nous préférerons garder les rapports de vulnérabilités privés jusqu'à ce que nous ayons eu l'occasion de les examiner et de les résoudre.

## Shout-outs / Remerciements

Many thanks to the contributors of these projects / Merci à tous les contributeurs des projets suivants:

- [krassowski/jupyterlab-lsp](https://github.com/jupyter-lsp/jupyterlab-lsp)
- [joe-re/sql-language-server](https://github.com/joe-re/sql-language-server)
- [zeroturnaround/sql-formatter](https://github.com/zeroturnaround/sql-formatter)
- [cryeo/sparksql-magic](https://github.com/cryeo/sparksql-magic)
- [trino-python-client](https://github.com/trinodb/trino-python-client)
- [bloomberg/ipydatagrid](https://github.com/bloomberg/ipydatagrid)
- [widgetti/ipyaggrid](https://github.com/widgetti/ipyaggrid)
