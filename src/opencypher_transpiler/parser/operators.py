"""Binary and unary operators for openCypher expressions."""

from enum import Enum, auto
from typing import NamedTuple


class BinaryOperator(Enum):
    """Binary operators supported by the transpiler."""

    INVALID = auto()

    # Numerical operators
    PLUS = auto()
    MINUS = auto()
    MULTIPLY = auto()
    DIVIDE = auto()
    MODULO = auto()
    EXPONENTIATION = auto()

    # Logical operators
    AND = auto()
    OR = auto()
    XOR = auto()

    # Comparison operators
    LT = auto()
    LEQ = auto()
    GT = auto()
    GEQ = auto()
    EQ = auto()
    NEQ = auto()
    REGMATCH = auto()
    IN = auto()


class BinaryOperatorType(Enum):
    """Classification of binary operators by their type semantics."""

    INVALID = auto()
    VALUE = auto()  # Takes value type (string/numeric) and outputs value type
    LOGICAL = auto()  # Takes logical type (bool) and outputs logical type
    COMPARISON = auto()  # Takes value type (string/numeric) and outputs logical type


class BinaryOperatorInfo(NamedTuple):
    """Information about a binary operator."""

    name: BinaryOperator
    operator_type: BinaryOperatorType

    def __str__(self) -> str:
        return self.name.name


# Mapping from operator symbols to their info
OPERATORS: dict[str, BinaryOperatorInfo] = {
    "+": BinaryOperatorInfo(BinaryOperator.PLUS, BinaryOperatorType.VALUE),
    "-": BinaryOperatorInfo(BinaryOperator.MINUS, BinaryOperatorType.VALUE),
    "*": BinaryOperatorInfo(BinaryOperator.MULTIPLY, BinaryOperatorType.VALUE),
    "/": BinaryOperatorInfo(BinaryOperator.DIVIDE, BinaryOperatorType.VALUE),
    "%": BinaryOperatorInfo(BinaryOperator.MODULO, BinaryOperatorType.VALUE),
    "^": BinaryOperatorInfo(BinaryOperator.EXPONENTIATION, BinaryOperatorType.VALUE),
    "<>": BinaryOperatorInfo(BinaryOperator.NEQ, BinaryOperatorType.COMPARISON),
    "=": BinaryOperatorInfo(BinaryOperator.EQ, BinaryOperatorType.COMPARISON),
    "<": BinaryOperatorInfo(BinaryOperator.LT, BinaryOperatorType.COMPARISON),
    ">": BinaryOperatorInfo(BinaryOperator.GT, BinaryOperatorType.COMPARISON),
    "<=": BinaryOperatorInfo(BinaryOperator.LEQ, BinaryOperatorType.COMPARISON),
    ">=": BinaryOperatorInfo(BinaryOperator.GEQ, BinaryOperatorType.COMPARISON),
    "=~": BinaryOperatorInfo(BinaryOperator.REGMATCH, BinaryOperatorType.COMPARISON),
    "in": BinaryOperatorInfo(BinaryOperator.IN, BinaryOperatorType.COMPARISON),
    "and": BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL),
    "or": BinaryOperatorInfo(BinaryOperator.OR, BinaryOperatorType.LOGICAL),
    "xor": BinaryOperatorInfo(BinaryOperator.XOR, BinaryOperatorType.LOGICAL),
}


def try_get_operator(op: str) -> BinaryOperatorInfo | None:
    """
    Try to get operator info for the given operator symbol.

    Args:
        op: The operator symbol (e.g., "+", "and").

    Returns:
        BinaryOperatorInfo if found, None otherwise.
    """
    return OPERATORS.get(op.lower() if op else "")


class AggregationFunction(Enum):
    """Aggregation functions supported by the transpiler."""

    INVALID = auto()
    NONE = auto()
    SUM = auto()
    AVG = auto()
    COUNT = auto()
    MAX = auto()
    MIN = auto()
    FIRST = auto()
    LAST = auto()
    PERCENTILE_CONT = auto()
    PERCENTILE_DISC = auto()
    STDEV = auto()
    STDEVP = auto()


def try_parse_aggregation_function(function_name: str) -> AggregationFunction | None:
    """
    Try to parse a function name into an aggregation function.

    Args:
        function_name: The function name to parse.

    Returns:
        AggregationFunction if it's an aggregation function, None otherwise.
    """
    mapping = {
        "avg": AggregationFunction.AVG,
        "sum": AggregationFunction.SUM,
        "count": AggregationFunction.COUNT,
        "max": AggregationFunction.MAX,
        "min": AggregationFunction.MIN,
        "first": AggregationFunction.FIRST,
        "last": AggregationFunction.LAST,
        "percentilecont": AggregationFunction.PERCENTILE_CONT,
        "percentiledisc": AggregationFunction.PERCENTILE_DISC,
        "stdev": AggregationFunction.STDEV,
        "stdevp": AggregationFunction.STDEVP,
    }
    return mapping.get(function_name.lower())


class Function(Enum):
    """Functions supported by the transpiler."""

    INVALID = auto()

    # Unary operators
    POSITIVE = auto()
    NEGATIVE = auto()
    NOT = auto()

    # Type conversion functions
    TO_FLOAT = auto()
    TO_STRING = auto()
    TO_BOOLEAN = auto()
    TO_INTEGER = auto()
    TO_LONG = auto()
    TO_DOUBLE = auto()

    # String functions
    STRING_STARTS_WITH = auto()
    STRING_ENDS_WITH = auto()
    STRING_CONTAINS = auto()
    STRING_LEFT = auto()
    STRING_RIGHT = auto()
    STRING_TRIM = auto()
    STRING_LTRIM = auto()
    STRING_RTRIM = auto()
    STRING_TO_UPPER = auto()
    STRING_TO_LOWER = auto()
    STRING_SIZE = auto()

    # Misc functions
    IS_NULL = auto()
    IS_NOT_NULL = auto()


class FunctionInfo(NamedTuple):
    """Information about a function."""

    function_name: Function
    required_parameters: int
    optional_parameters: int = 0


# Mapping from function names to their info
FUNCTIONS: dict[str, FunctionInfo] = {
    "+": FunctionInfo(Function.POSITIVE, 1),
    "-": FunctionInfo(Function.NEGATIVE, 1),
    "not": FunctionInfo(Function.NOT, 1),
    "tofloat": FunctionInfo(Function.TO_FLOAT, 1),
    "todouble": FunctionInfo(Function.TO_DOUBLE, 1),
    "tostring": FunctionInfo(Function.TO_STRING, 1),
    "toboolean": FunctionInfo(Function.TO_BOOLEAN, 1),
    "tointeger": FunctionInfo(Function.TO_INTEGER, 1),
    "tolong": FunctionInfo(Function.TO_LONG, 1),
    "startswith": FunctionInfo(Function.STRING_STARTS_WITH, 2),
    "endswith": FunctionInfo(Function.STRING_ENDS_WITH, 2),
    "contains": FunctionInfo(Function.STRING_CONTAINS, 2),
    "left": FunctionInfo(Function.STRING_LEFT, 2),
    "right": FunctionInfo(Function.STRING_RIGHT, 2),
    "trim": FunctionInfo(Function.STRING_TRIM, 1),
    "ltrim": FunctionInfo(Function.STRING_LTRIM, 1),
    "rtrim": FunctionInfo(Function.STRING_RTRIM, 1),
    "toupper": FunctionInfo(Function.STRING_TO_UPPER, 1),
    "tolower": FunctionInfo(Function.STRING_TO_LOWER, 1),
    "size": FunctionInfo(Function.STRING_SIZE, 1),
}


def try_get_function(function_name: str) -> FunctionInfo | None:
    """
    Try to get function info for the given function name.

    Args:
        function_name: The function name.

    Returns:
        FunctionInfo if found, None otherwise.
    """
    return FUNCTIONS.get(function_name.lower() if function_name else "")
