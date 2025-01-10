## DEPRECATED
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
    TimestampNTZType,
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
    "timestamp": "TIMESTAMP",
    "with": "WITH",
    "time": "TIME",
    "zone": "ZONE",
}

# Token definitions
tokens = [
    "LPAREN",
    "RPAREN",
    "COMMA",
    "NAME_PART",
    "SPACE",
] + list(reserved.values())

# Token regex for reserved keywords
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_COMMA = r","
t_WITH = r"with"
t_TIME = r"time"
t_ZONE = r"zone"

# A string containing ignored characters (spaces and tabs)
t_ignore = " \t"


# Token regex for NAME_PART (non-reserved words)
def t_NAME_PART(t):
    r"[^\s,()]+"  # Matches valid identifiers (letters, digits, underscores)
    t.type = reserved.get(t.value.lower(), "NAME_PART")
    return t


def t_TIMESTAMP(t):
    r"timestamp"
    return t


def t_error(t):
    print(f"Illegal character '{t.value[0]}' at position {t.lexpos}")
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
    "type : MAP LPAREN type COMMA type RPAREN"
    p[0] = MapType(p[3], p[5])


def p_row(p):
    "type : ROW LPAREN row_field_list RPAREN"
    p[0] = StructType(p[3])


def p_row_field_list(p):
    """
    row_field_list : row_field_list COMMA row_field
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
        # Accumulate name parts and the field type
        field_name = f"{p[1]} {p[2]['field_name']}".strip()
        p[0] = {"field_name": field_name, "field_type": p[2]["field_type"]}
    elif len(p) == 2:
        # Initialize field with type only
        p[0] = {"field_name": "", "field_type": p[1]}


# a field part are the tokens inside a row(thename with spaces and reserved words timestamp)
# the left most token will be the field type, all others form the name of the field
# the name can contain tokens such as a space, with, row, integer
# so a field_part can be a type or the type_name that is 'integer'
def p_field_part(p):
    """
    field_part :  NAME_PART
                | NAME_PART SPACE field_part
                | NAME_PART LPAREN NAME_PART RPAREN
                | NAME_PART LPAREN NAME_PART RPAREN SPACE field_part
                | SPACE
                | WITH
                | ZONE
                | ROW
                | MAP
                | type
    """
    if len(p) == 2:
        p[0] = p[1]  # Simple case for NAME_PART
    elif len(p) == 4:
        p[0] = f"{p[1]} {p[3]}"  # Concatenated with SPACE
    elif len(p) == 5:
        p[0] = f"{p[1]} ({p[3]})"  # Concatenated with parentheses
    elif len(p) == 7:
        p[0] = f"{p[1]} ({p[3]}) {p[6]}"  # Parentheses and additional part


def p_digits(t):
    """digits : NAME_PART"""
    if t[1].isdigit():
        t[0] = int(t[1])  # Convert the numeric value to an integer
    else:
        raise SyntaxError(f"Expected numeric value, got: {t[1]}")


def p_type_decimal(t):
    """type : DECIMAL
    | DECIMAL LPAREN digits RPAREN
    | DECIMAL LPAREN digits COMMA digits RPAREN"""
    if len(t) == 2:
        # No precision/scale provided, default to 10, 0
        t[0] = DecimalType(10, 0)
    elif len(t) == 5:
        # Precision only
        t[0] = DecimalType(int(t[3]), 0)
    else:
        # Precision and scale
        t[0] = DecimalType(int(t[3]), int(t[5]))


def p_type_timestamp(t):
    """type : TIMESTAMP
    | TIMESTAMP LPAREN digits RPAREN
    | TIMESTAMP LPAREN digits RPAREN WITH TIME ZONE
    | TIMESTAMP WITH TIME ZONE"""
    if len(t) == 2:  # "timestamp"
        t[0] = TimestampNTZType()  # Non-Time-Zone-Aware Timestamp
    elif len(t) == 4:  # "timestamp with time zone"
        t[0] = TimestampType()  # Time-Zone-Aware Timestamp
    elif len(t) == 5:  # "timestamp(precision)"
        t[0] = TimestampNTZType()
    elif len(t) == 8:  # "timestamp(precision) with time zone"
        t[0] = TimestampType()
    else:
        raise SyntaxError(f"Invalid timestamp type definition: {t}")


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
                | VARCHAR
                | VARCHAR LPAREN digits RPAREN
                | CHAR
                | VARBINARY
                | JSON
                | DATE
                | TIME
    """
    p[0] = p[1]


# Error rule for syntax errors
def p_error(p):
    if p:
        print(f"Syntax error at token '{p.type}', value '{p.value}', at position {p.lexpos}")
    else:
        print("Syntax error in input!")


# Build the parser
trino_column_parser = yacc.yacc()
