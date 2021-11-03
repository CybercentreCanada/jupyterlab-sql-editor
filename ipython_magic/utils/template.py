from jinja2 import Template, StrictUndefined



VARIABLE_NOT_FOUND_MSG = '''
A Jinja template variable named {{{var_name}}} was located in your SQL statement.

However Jinja was unable to substitute it's value because the variable "{var_name}" was not found in your ipython kernel.

Option 1: If you intended to use a template variable make sure to assign a value to "{var_name}"
'''

HOW_TO_ESCAPE_MSG = '''
Option 2: If you intended to include "{{" in your statement then you'll need to escape this special Jinja variable delimitere.

To have Jinja ignore parts it would otherwise handle as variables or blocks. For example, if, with the default syntax, you want to use {{ as a raw string in a template and not start a variable, you have to use a trick.

The easiest way to output a literal variable delimiter "{{" is by using a variable expression:

{{ '{{' }}

For bigger sections, it makes sense to mark a block raw. For example, to include example Jinja syntax in a template, you can use this snippet:

{% raw %}
    <ul>
    {% for item in seq %}
        <li>{{ item }}</li>
    {% endfor %}
    </ul>
{% endraw %}
'''

RAISING_ERROR_MSG = "Raising an error to prevent statement from being executed incorrectly."

class ExplainUndefined(StrictUndefined):
    __slots__ = ()

    def __str__(self) -> str:
        print(VARIABLE_NOT_FOUND_MSG.format(var_name=self._undefined_name))
        print(HOW_TO_ESCAPE_MSG)
        print(RAISING_ERROR_MSG)
        return super().__str__()


def bind_variables(query, user_ns):
    t = Template(query, undefined=ExplainUndefined)
    ret = t.render(user_ns)
    return ret
