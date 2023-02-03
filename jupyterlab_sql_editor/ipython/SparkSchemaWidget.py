from ipytree import Node, Tree
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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


class SparkSchemaWidget(Tree):
    def __init__(self, name, schema) -> None:
        super(SparkSchemaWidget, self).__init__()
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
        if isinstance(field, StructField):
            return self.get_children(field.dataType, field.name)
        elif isinstance(field, MapType):
            key = self.get_children(field.keyType, "key")
            value = self.get_children(field.valueType, "value")
            nodes = [key, value]
            return Node(f"{name}: map", nodes, icon=icons["map"], **self.complex_type)
        elif isinstance(field, ArrayType):
            element = self.get_children(field.elementType, "element")
            nodes = [element]
            return Node(f"{name}: array", nodes, icon=icons["array"], **self.complex_type)
        elif isinstance(field, StructType):
            nodes = [self.get_children(f, "") for f in field.fields]
            return Node(f"{name}: struct", nodes, icon=icons["struct"], **self.complex_type)
        elif isinstance(field, StringType):
            return Node(f"{name}: string", icon=icons["string"])
        elif isinstance(field, TimestampType):
            return Node(f"{name}: timestamp", icon=icons["time"])
        elif isinstance(field, DateType):
            return Node(f"{name}: date", icon=icons["date"])
        elif isinstance(field, LongType):
            return Node(f"{name}: long", icon=icons["integer"])
        elif isinstance(field, IntegerType):
            return Node(f"{name}: integer", icon=icons["integer"])
        elif isinstance(field, BooleanType):
            return Node(f"{name}: boolean", icon=icons["boolean"])
        elif isinstance(field, DecimalType):
            return Node(f"{name}: decimal", icon=icons["decimal"])
        elif isinstance(field, DoubleType):
            return Node(f"{name}: double", icon=icons["decimal"])
        elif isinstance(field, FloatType):
            return Node(f"{name}: float", icon=icons["decimal"])
        elif isinstance(field, ShortType):
            return Node(f"{name}: short", icon=icons["integer"])
        elif isinstance(field, BinaryType):
            return Node(f"{name}: binary", icon=icons["binary"])

    def to_tree(self):
        return self
