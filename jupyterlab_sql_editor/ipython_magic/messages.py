VARIABLE_NOT_FOUND_MSG = """
A Jinja template variable named {{{var_name}}} was located in your SQL statement.
However, Jinja was unable to substitute it's value because the variable "{var_name}" was not found in your IPython kernel.
If you intended to use a template variable, make sure to assign a value to "{var_name}"
"""

HOW_TO_ESCAPE_MSG = """
If you intended to include "{{" in your statement then you'll need to escape this special Jinja variable delimiter
to have Jinja ignore parts it would otherwise handle as variables or blocks. For example, if you want to use {{ as
a raw string in a template and not start a variable with the default syntax, you will have to use a trick.
The easiest way to output a literal variable delimiter "{{" is by using a variable expression:

{{ '{{' }}

For bigger sections, it makes sense to mark a block raw. For example, you can use this snippet to include example
Jinja syntax in a template:

%%trino --limit 3
{% raw %}
/*
This is a comment which happens to contain a jinja template
variable {{x}} that we want to keep as is.
*/
{% endraw %}

SELECT
    *
FROM
    {{ table_name }}
"""
