# jupyterlab-sql-editor

Une extension à JupyterLab qui contribue:

- Formattage de SQL
- Extraction automatique du schéma de base de données
- Complétion automatique suivant un `tab` ou `dot` des:
  - noms de tables
  - alias de tables
  - jointures de tables
  - noms de colonnes imbriquées
  - fonctions
- Surbrillance de la syntaxe:
  - ligne magic
  - cellule magic
  - chaînes de caractères Python

IPython magic pour:

- Spark SQL
- Trino

## Execution et affichage des résultats dans une grille interactive

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//ipydatagrid.gif)

## Résultats en JSON

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//json-output.gif)

## Suggestion automatique des noms de colones et sous champs

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//sparksql-nested-columns.gif)

## Suggestion automatique de jointures sur nom de colonnes

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//spark-inner-join.gif)

## Formattage et surbrillance de la syntaxe des cellules

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images/format-cell.gif)

## Fonctionne à l'intérieur d'une chaînes de caractères Python

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//python-string.gif)

## Capture de la requête dans un Dataframe ou vue temporaire

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//args.png)

## Utilisation de Jinja pour création de requêtes SQL réutilisables

![](https://raw.githubusercontent.com/CybercentreCanada/jupyterlab-sql-editor/main/images//jinja.png)

## Installation

Suivre les instructions d'installation dans [CONTRIBUTING](./CONTRIBUTING.md)

## Paramètres

Exemple d'usage:

```
%%sparksql -c -l 10 --dataframe df
<QUERY>
```

### sparksql

| Parameter                                                                                                                       | Description                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `--database NAME`                                                                                                               | Spark database to use.                                                                                                                        |
| `-l LIMIT` `--limit LIMIT`                                                                                                      | The maximum number of rows to display. A value of zero is equivalent to `--output skip`                                                       |
| `-r all\|local\|none` `--refresh all\|local\|none`                                                                              | Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.         |
| `-d NAME` `--dataframe NAME`                                                                                                    | Capture dataframe in a local variable named `NAME`.                                                                                           |
| `-c` `--cache`                                                                                                                  | Cache dataframe.                                                                                                                              |
| `-e` `--eager`                                                                                                                  | Cache dataframe with eager load.                                                                                                              |
| `-v VIEW` `--view VIEW`                                                                                                         | Create or replace a temporary view named `VIEW`.                                                                                              |
| `-o sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` `--output sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` | Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements). |
| `-s` `--show-nonprinting`                                                                                                       | Replace none printable characters with their ascii codes (`LF` -> `\x0a`)                                                                     |
| `-j` `--jinja`                                                                                                                  | Enable Jinja templating support.                                                                                                              |
| `-b` `--dbt`                                                                                                                    | Enable DBT templating support.                                                                                                                |
| `-t LIMIT` `--truncate LIMIT`                                                                                                   | Truncate output.                                                                                                                              |
| `-m update\|complete` `--streaming_mode update\|complete`                                                                       | The mode of streaming queries.                                                                                                                |
| `-x` `--lean-exceptions`                                                                                                        | Shortened exceptions. Might be helpful if the exceptions reported by Spark are noisy such as with big SQL queries.                            |

### trino

| Parameter                                                                                                                       | Description                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-c NAME` `--catalog NAME`                                                                                                      | Trino catalog to use.                                                                                                                                                                         |
| `-s NAME` `--schema NAME`                                                                                                       | Trino schema to use.                                                                                                                                                                          |
| `-l LIMIT` `--limit LIMIT`                                                                                                      | The maximum number of rows to display. A value of zero is equivalent to `--output skip`                                                                                                       |
| `-r all\|none` `--refresh all\|none`                                                                                            | Force the regeneration of the schema cache file.                                                                                                                                              |
| `-d NAME` `--dataframe NAME`                                                                                                    | Capture dataframe in a local variable named `NAME`.                                                                                                                                           |
| `-o sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` `--output sql\|json\|html\|aggrid\|grid\|text\|schema\|skip\|none` | Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements).                                                 |
| `-s` `--show-nonprinting`                                                                                                       | Replace none printable characters with their ascii codes (`LF` -> `\x0a`).                                                                                                                    |
| `-j` `--jinja`                                                                                                                  | Enable Jinja templating support.                                                                                                                                                              |
| `-t LIMIT` `--truncate LIMIT`                                                                                                   | Truncate output.                                                                                                                                                                              |
| `-x STATEMENT` `--raw STATEMENT`                                                                                                | Run statement as is. Do not wrap statement with a limit. Use this option to run statement which can't be wrapped in a SELECT/LIMIT statement. For example EXPLAIN, SHOW TABLE, SHOW CATALOGS. |

## Rapports sur les vulnérabilités de sécurité

Si vous pensez avoir identifié une faille de sécurité dans ce projet, veuillez envoyer un courriel à l'équipe du projet à contact@cyber.gc.ca, en détaillant le problème soupçonné et les méthodes que vous avez trouvées pour le reproduire.

Veuillez NE PAS ouvrir un problème dans le GitHub repo, car nous préférerons garder les rapports de vulnérabilités privés jusqu'à ce que nous ayons eu l'occasion de les examiner et de les résoudre.

## Remerciements

Merci à tous les contributeurs des projets suivants:

- [krassowski/jupyterlab-lsp](https://github.com/jupyter-lsp/jupyterlab-lsp)
- [joe-re/sql-language-server](https://github.com/joe-re/sql-language-server)
- [zeroturnaround/sql-formatter](https://github.com/zeroturnaround/sql-formatter)
- [cryeo/sparksql-magic](https://github.com/cryeo/sparksql-magic)
- [trino-python-client](https://github.com/trinodb/trino-python-client)
- [bloomberg/ipydatagrid](https://github.com/bloomberg/ipydatagrid)
- [widgetti/ipyaggrid](https://github.com/widgetti/ipyaggrid)
