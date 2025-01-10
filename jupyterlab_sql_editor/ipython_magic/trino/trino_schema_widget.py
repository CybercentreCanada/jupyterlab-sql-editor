from ipytree import Node, Tree
from sqlalchemy.sql import sqltypes
from trino.sqlalchemy.datatype import DOUBLE, JSON, MAP, ROW, TIME, TIMESTAMP

icons = {
    "time": "clock",
    "date": "calendar",
    "string": "at",
    "decimal": "percentage",
    "integer": "hashtag",
    "boolean": "check-circle",
    "binary": "delicious",
    "struct": "project-diagram",
    "array": "language",
    "map": "key",
}


class TrinoSchemaWidget(Tree):
    def __init__(self, name, schema) -> None:
        super(TrinoSchemaWidget, self).__init__()
        node = self.get_children(schema, name)
        self.add_node(node)

    complex_type = {
        "opened": False,
        "open_icon_style": "danger",
        "close_icon_style": "danger",
        "icon_style": "success",
        "open_icon": "angle-right",
        "close_icon": "angle-down",
    }

    def get_children(self, field, name):
        if type(field) is dict:
            return self.get_children(field["type"], field["columnName"])
        if type(field) is tuple:
            return self.get_children(field[1], field[0])
        if type(field) is MAP:
            key = self.get_children(field.key_type, "key")
            value = self.get_children(field.value_type, "value")
            nodes = [key, value]
            return Node(f"{name}: MAP", nodes, icon=icons["map"], **self.complex_type)
        elif type(field) is sqltypes.ARRAY:
            element = self.get_children(field.item_type, "element")
            nodes = [element]
            return Node(f"{name}: ARRAY", nodes, icon=icons["array"], **self.complex_type)
        elif type(field) is list:
            nodes = [self.get_children(f, "") for f in field]
            return Node(f"{name}", nodes, icon=icons["struct"], **self.complex_type)
        elif type(field) is ROW:
            nodes = [self.get_children(f, "") for f in field.attr_types]
            return Node(f"{name}: ROW", nodes, icon=icons["struct"], **self.complex_type)
        elif type(field) is sqltypes.VARCHAR:
            return Node(f"{name}: VARCHAR", icon=icons["string"])
        elif type(field) is sqltypes.CHAR:
            return Node(f"{name}: CHAR", icon=icons["string"])
        elif type(field) is sqltypes.VARBINARY:
            return Node(f"{name}: VARBINARY", icon=icons["string"])
        elif type(field) is JSON:
            return Node(f"{name}: JSON", icon=icons["string"])
        elif type(field) is sqltypes.DATE:
            return Node(f"{name}: DATE", icon=icons["date"])
        elif type(field) is TIME:
            return Node(f"{name}: TIME", icon=icons["time"])
        elif type(field) is TIMESTAMP:
            return Node(f"{name}: TIMESTAMP", icon=icons["time"])
        elif type(field) is sqltypes.SMALLINT:
            return Node(f"{name}: SMALLINT", icon=icons["integer"])
        elif type(field) is sqltypes.INTEGER:
            return Node(f"{name}: INTEGER", icon=icons["integer"])
        elif type(field) is sqltypes.BIGINT:
            return Node(f"{name}: BIGINT", icon=icons["integer"])
        elif type(field) is sqltypes.BOOLEAN:
            return Node(f"{name}: BOOLEAN", icon=icons["boolean"])
        elif type(field) is sqltypes.REAL:
            return Node(f"{name}: REAL", icon=icons["decimal"])
        elif type(field) is sqltypes.DECIMAL:
            return Node(f"{name}: DECIMAL", icon=icons["decimal"])
        elif type(field) is DOUBLE:
            return Node(f"{name}: DOUBLE", icon=icons["decimal"])
        else:
            print(f"Type doesn't match anything in the list. Type: {type(field)}")
            return Node("None", icon=icons["binary"])

    def to_tree(self):
        return self
