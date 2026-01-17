"""Test utilities for gsql2rsql transpiler tests."""

from tests.utils.sql_test_utils import (
    normalize_sql,
    assert_sql_equal,
    write_golden_if_missing,
    load_expected_sql,
    TranspilerTestCase,
)
from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
    assert_has_join,
    assert_no_join,
    assert_has_order_by,
    assert_has_limit_offset,
    assert_has_group_by,
    assert_has_recursive_cte,
    assert_has_array_agg,
    assert_projected_columns,
    assert_no_cartesian_join,
    SQLStructure,
)

__all__ = [
    # Core utilities
    "normalize_sql",
    "assert_sql_equal",
    "write_golden_if_missing",
    "load_expected_sql",
    "TranspilerTestCase",
    # Structural assertions
    "assert_has_select",
    "assert_has_from_table",
    "assert_has_where",
    "assert_has_join",
    "assert_no_join",
    "assert_has_order_by",
    "assert_has_limit_offset",
    "assert_has_group_by",
    "assert_has_recursive_cte",
    "assert_has_array_agg",
    "assert_projected_columns",
    "assert_no_cartesian_join",
    "SQLStructure",
]
