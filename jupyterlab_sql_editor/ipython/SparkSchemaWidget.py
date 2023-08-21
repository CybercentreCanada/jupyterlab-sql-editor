from ipytree import Node, Tree
from pyspark.sql.types import (
    ArrayType,
    AtomicType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    FractionalType,
    IntegerType,
    IntegralType,
    LongType,
    MapType,
    NullType,
    NumericType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
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
            return Node(f"{name}: Map", nodes, icon=icons["map"], **self.complex_type)
        elif isinstance(field, ArrayType):
            element = self.get_children(field.elementType, "element")
            nodes = [element]
            return Node(f"{name}: Array", nodes, icon=icons["array"], **self.complex_type)
        elif isinstance(field, StructType):
            nodes = [self.get_children(f, "") for f in field.fields]
            return Node(f"{name}: Struct", nodes, icon=icons["struct"], **self.complex_type)
        elif isinstance(field, StringType):
            return Node(f"{name}: String", icon=icons["string"])
        elif isinstance(field, TimestampType):
            return Node(f"{name}: Timestamp", icon=icons["time"])
        elif isinstance(field, TimestampNTZType):
            return Node(f"{name}: TimestampNTZ", icon=icons["time"])
        elif isinstance(field, DateType):
            return Node(f"{name}: Date", icon=icons["date"])
        elif isinstance(field, DayTimeIntervalType):
            return Node(f"{name}: DayTimeInternval", icon=icons["date"])
        elif isinstance(field, LongType):
            return Node(f"{name}: Long", icon=icons["integer"])
        elif isinstance(field, IntegerType):
            return Node(f"{name}: Integer", icon=icons["integer"])
        elif isinstance(field, NullType):
            return Node(f"{name}: Null", icon=icons["boolean"])
        elif isinstance(field, BooleanType):
            return Node(f"{name}: Boolean", icon=icons["boolean"])
        elif isinstance(field, AtomicType):
            return Node(f"{name}: Atomic", icon=icons["boolean"])
        elif isinstance(field, NumericType):
            return Node(f"{name}: Numeric", icon=icons["decimal"])
        elif isinstance(field, IntegralType):
            return Node(f"{name}: Integral", icon=icons["decimal"])
        elif isinstance(field, DecimalType):
            return Node(f"{name}: Decimal", icon=icons["decimal"])
        elif isinstance(field, DoubleType):
            return Node(f"{name}: Double", icon=icons["decimal"])
        elif isinstance(field, FloatType):
            return Node(f"{name}: Float", icon=icons["decimal"])
        elif isinstance(field, FractionalType):
            return Node(f"{name}: Fractional", icon=icons["decimal"])
        elif isinstance(field, ShortType):
            return Node(f"{name}: Short", icon=icons["integer"])
        elif isinstance(field, DataType):
            return Node(f"{name}: Data", icon=icons["binary"])
        elif isinstance(field, BinaryType):
            return Node(f"{name}: Binary", icon=icons["binary"])
        elif isinstance(field, ByteType):
            return Node(f"{name}: Byte", icon=icons["binary"])
        elif isinstance(field, UserDefinedType):
            return Node(f"{name}: UserDefined", icon=icons["binary"])

    def to_tree(self):
        return self
