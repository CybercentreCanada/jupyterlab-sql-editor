# ------------------------------------------------------------
# tokenizer for a simple trino column schema expression
# of the form row(x 1 integer, y varchar(12), z array(varchar))
# ------------------------------------------------------------
import ply.lex as lex
import ply.yacc as yacc
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
    StringType,
    StructField,
    StructType,
    TimestampType,
)

scalar_type_map = {
    "boolean": BooleanType(),
    "tinyint": IntegerType(),
    "smallint": IntegerType(),
    "integer": IntegerType(),
    "bigint": LongType(),
    "real": FloatType(),
    "double": DoubleType(),
    "decimal": DecimalType(),
    "varchar": StringType(),
    "char": StringType(),
    "varbinary": BinaryType(),
    "json": StringType(),
    "date": DateType(),
    "time": TimestampType(),
    "timestamp": TimestampType(),
}

reserved = {
    "row": "ROW",
    "array": "ARRAY",
    "map": "MAP",
    "boolean": "BOOLEAN",
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "real": "REAL",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "varchar": "VARCHAR",
    "char": "CHAR",
    "varbinary": "VARBINARY",
    "json": "JSON",
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMP",
    "with": "WITH",
    "zone": "ZONE",
}

# List of token names.   This is always required
tokens = [
    "LPAREN",
    "RPAREN",
    "COMMA_SPACE",
    "NAME_PART",
    "SPACE",
    "NUMBER",
] + list(reserved.values())

# Regular expression rules for simple tokens
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_COMMA_SPACE = r",\ "
t_SPACE = r"\ "


def t_NAME_PART(t):
    r"[^\(\) ,]+"
    if t.value.isnumeric():
        t.type = "NUMBER"
    else:
        t.type = reserved.get(t.value, "NAME_PART")
    t.value = str(t.value)
    return t


# A string containing ignored characters (spaces and tabs)
t_ignore = ""

# Error handling rule
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


# Build the lexer
trino_column_lexer = lex.lex()


# ------------------------------------------------------------
# parser for a simple trino column schema expression
# of the form row(x 1 integer, y varchar(12), z array(varchar))
# ------------------------------------------------------------

# Yacc
def p_array(p):
    "type : ARRAY LPAREN type RPAREN"
    p[0] = ArrayType(p[3])


def p_map(p):
    "type : MAP LPAREN type COMMA_SPACE type RPAREN"
    p[0] = MapType(p[3], p[5])


def p_row(p):
    "type : ROW LPAREN row_field_list RPAREN"
    p[0] = StructType(p[3])


def p_row_field_list(p):
    """
    row_field_list : row_field_list COMMA_SPACE row_field
                   | row_field
    """
    if len(p) == 4:
        # add additional field
        flist = []
        flist.extend(p[1])
        flist.append(p[3])
        p[0] = flist
    elif len(p) == 2:
        # single field list
        p[0] = [p[1]]


def p_row_field(p):
    """
    row_field : row_field_parts
    """
    field_name = p[1]["field_name"]
    field_type = p[1]["field_type"]
    p[0] = StructField(field_name, field_type)


def p_row_field_parts(p):
    """
    row_field_parts : field_part row_field_parts
                    | type
    """
    if len(p) == 3:
        if p[2]["field_name"] == "" and p[1] == " ":
            # we have a type only and we got the first space character, pass over it
            p[0] = p[2]
        else:
            # prefix the field_part onto the field_name we are building
            name = p[1] + p[2]["field_name"]
            # row_field_parts is carrying the field_type
            p[0] = {"field_name": name, "field_type": p[2]["field_type"]}

    elif len(p) == 2:
        # start by recieving the type of the field
        # name is unknown at this point
        p[0] = {"field_name": "", "field_type": p[1]}


# a field part are the tokens inside a row(thename with spaces and reserved words timestamp)
# the left most token will be the field type, all others form the name of the field
# the name can contain tokens such as a space, with, row, integer
# so a field_part can be a type or the type_name that is 'integer'
def p_field_part(p):
    """
    field_part :  NAME_PART
                | SPACE
                | WITH
                | ZONE
                | ROW
                | MAP
                | type
                | type_name

    """
    p[0] = p[1]


def p_type(p):
    "type : type_name"
    # lookup the type object
    the_type = scalar_type_map[p[1]]
    p[0] = the_type


def p_type_name(p):
    """
    type_name :   BOOLEAN
                | TINYINT
                | SMALLINT
                | INTEGER
                | BIGINT
                | REAL
                | DOUBLE
                | DECIMAL
                | VARCHAR
                | VARCHAR LPAREN NUMBER RPAREN
                | CHAR
                | VARBINARY
                | JSON
                | DATE
                | TIME
                | TIMESTAMP
                | TIMESTAMP LPAREN NUMBER RPAREN SPACE WITH SPACE TIME SPACE ZONE
    """
    p[0] = p[1]


# Error rule for syntax errors
def p_error(p):
    print("Syntax error in input!")


# Build the parser
trino_column_parser = yacc.yacc()
