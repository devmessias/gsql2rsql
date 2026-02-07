"""Databricks SQL dialect constants.

Type mappings, operator patterns, and aggregation patterns for the
Databricks SQL target dialect. These are pure data — no logic.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import Any

from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperator,
)


class DatabricksSqlType(Enum):
    """Databricks SQL data types."""

    INT = auto()
    SMALLINT = auto()
    BIGINT = auto()
    DOUBLE = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    BINARY = auto()
    DECIMAL = auto()
    DATE = auto()
    TIMESTAMP = auto()
    ARRAY = auto()
    MAP = auto()
    STRUCT = auto()


# Mapping from Python types to Databricks SQL types
TYPE_TO_SQL_TYPE: dict[type[Any], DatabricksSqlType] = {
    int: DatabricksSqlType.BIGINT,
    float: DatabricksSqlType.DOUBLE,
    str: DatabricksSqlType.STRING,
    bool: DatabricksSqlType.BOOLEAN,
    bytes: DatabricksSqlType.BINARY,
    list: DatabricksSqlType.ARRAY,
    dict: DatabricksSqlType.MAP,
}

# Mapping from Databricks SQL types to their string representations
SQL_TYPE_RENDERING: dict[DatabricksSqlType, str] = {
    DatabricksSqlType.INT: "INT",
    DatabricksSqlType.SMALLINT: "SMALLINT",
    DatabricksSqlType.BIGINT: "BIGINT",
    DatabricksSqlType.DOUBLE: "DOUBLE",
    DatabricksSqlType.FLOAT: "FLOAT",
    DatabricksSqlType.STRING: "STRING",
    DatabricksSqlType.BOOLEAN: "BOOLEAN",
    DatabricksSqlType.BINARY: "BINARY",
    DatabricksSqlType.DECIMAL: "DECIMAL(38, 10)",
    DatabricksSqlType.DATE: "DATE",
    DatabricksSqlType.TIMESTAMP: "TIMESTAMP",
    DatabricksSqlType.ARRAY: "ARRAY<STRING>",
    DatabricksSqlType.MAP: "MAP<STRING, STRING>",
    DatabricksSqlType.STRUCT: "STRUCT<>",
}

# Mapping from binary operators to SQL patterns (Databricks compatible)
OPERATOR_PATTERNS: dict[BinaryOperator, str] = {
    BinaryOperator.PLUS: "({0}) + ({1})",
    BinaryOperator.MINUS: "({0}) - ({1})",
    BinaryOperator.MULTIPLY: "({0}) * ({1})",
    BinaryOperator.DIVIDE: "({0}) / ({1})",
    BinaryOperator.MODULO: "({0}) % ({1})",
    BinaryOperator.EXPONENTIATION: "POWER({0}, {1})",
    BinaryOperator.AND: "({0}) AND ({1})",
    BinaryOperator.OR: "({0}) OR ({1})",
    BinaryOperator.XOR: "(({0}) AND NOT ({1})) OR (NOT ({0}) AND ({1}))",
    BinaryOperator.LT: "({0}) < ({1})",
    BinaryOperator.LEQ: "({0}) <= ({1})",
    BinaryOperator.GT: "({0}) > ({1})",
    BinaryOperator.GEQ: "({0}) >= ({1})",
    BinaryOperator.EQ: "({0}) = ({1})",
    BinaryOperator.NEQ: "({0}) != ({1})",
    BinaryOperator.REGMATCH: "({0}) RLIKE ({1})",
    BinaryOperator.IN: "({0}) IN {1}",
}

# Mapping from aggregation functions to SQL patterns (Databricks compatible)
AGGREGATION_PATTERNS: dict[AggregationFunction, str] = {
    AggregationFunction.AVG: "AVG(CAST({0} AS DOUBLE))",
    AggregationFunction.SUM: "SUM({0})",
    AggregationFunction.MIN: "MIN({0})",
    AggregationFunction.MAX: "MAX({0})",
    AggregationFunction.FIRST: "FIRST({0})",
    AggregationFunction.LAST: "LAST({0})",
    AggregationFunction.STDEV: "STDDEV({0})",
    AggregationFunction.STDEVP: "STDDEV_POP({0})",
    AggregationFunction.COUNT: "COUNT({0})",
    AggregationFunction.COLLECT: "COLLECT_LIST({0})",
}
