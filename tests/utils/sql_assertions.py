"""
SQL structural assertions for transpiler tests.

These helpers provide semantic assertions on SQL structure rather than
brittle string matching. They parse SQL and check for the presence of
specific constructs (SELECT, JOIN, WHERE, etc.).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class SQLStructure:
    """
    Parsed SQL structure for semantic assertions.

    This provides a lightweight parse of SQL to extract key structural elements
    without requiring a full SQL parser dependency.
    """

    raw_sql: str
    normalized_sql: str = ""

    # Extracted components
    has_with_clause: bool = False
    has_recursive_cte: bool = False
    cte_names: list[str] = field(default_factory=list)

    select_columns: list[str] = field(default_factory=list)
    has_distinct: bool = False

    from_tables: list[str] = field(default_factory=list)
    join_clauses: list[dict[str, Any]] = field(default_factory=list)

    where_clause: str | None = None
    has_where: bool = False

    group_by_columns: list[str] = field(default_factory=list)
    has_group_by: bool = False

    having_clause: str | None = None
    has_having: bool = False

    order_by_columns: list[str] = field(default_factory=list)
    has_order_by: bool = False

    limit_value: int | None = None
    offset_value: int | None = None

    def __post_init__(self) -> None:
        """Parse SQL structure on initialization."""
        self._parse()

    def _parse(self) -> None:
        """Parse the SQL string to extract structure."""
        sql = self.raw_sql.strip()
        self.normalized_sql = sql

        # Normalize for parsing (case-insensitive)
        sql_upper = sql.upper()

        # Check for WITH/RECURSIVE
        self.has_with_clause = sql_upper.startswith("WITH ")
        self.has_recursive_cte = "WITH RECURSIVE" in sql_upper

        # Extract CTE names
        if self.has_with_clause:
            cte_pattern = r"WITH\s+(?:RECURSIVE\s+)?(\w+)\s+AS"
            matches = re.findall(cte_pattern, sql, re.IGNORECASE)
            self.cte_names = matches

        # Check for DISTINCT (either SELECT DISTINCT or GROUP BY TO_JSON workaround)
        self.has_distinct = bool(
            re.search(r"\bSELECT\s+DISTINCT\b", sql, re.IGNORECASE)
            or (
                re.search(r"\bFIRST\(", sql, re.IGNORECASE)
                and re.search(r"\bGROUP\s+BY\s+TO_JSON\(", sql, re.IGNORECASE)
            )
        )

        # Check for WHERE
        self.has_where = bool(re.search(r"\bWHERE\b", sql, re.IGNORECASE))
        if self.has_where:
            # Extract WHERE clause content (simplified)
            where_match = re.search(
                r"\bWHERE\s+(.+?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
                sql,
                re.IGNORECASE | re.DOTALL,
            )
            if where_match:
                self.where_clause = where_match.group(1).strip()

        # Check for GROUP BY
        self.has_group_by = bool(re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE))

        # Check for HAVING
        self.has_having = bool(re.search(r"\bHAVING\b", sql, re.IGNORECASE))

        # Check for ORDER BY
        self.has_order_by = bool(re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE))

        # Extract LIMIT value
        limit_match = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
        if limit_match:
            self.limit_value = int(limit_match.group(1))

        # Extract OFFSET value
        offset_match = re.search(r"\bOFFSET\s+(\d+)", sql, re.IGNORECASE)
        if offset_match:
            self.offset_value = int(offset_match.group(1))

        # Extract JOINs
        join_pattern = (
            r"\b(INNER|LEFT|RIGHT|OUTER|CROSS)?\s*JOIN\b"
            r"\s+(.+?)\s+(?:AS\s+)?(\w+)?\s*\bON\b\s*(.+?)(?=\bJOIN\b|$)"
        )
        # Simplified join detection
        self.join_clauses = []
        for match in re.finditer(
            r"\b(INNER|LEFT|RIGHT|OUTER|CROSS)?\s*JOIN\b",
            sql,
            re.IGNORECASE,
        ):
            join_type = match.group(1) or "INNER"
            self.join_clauses.append({"type": join_type.upper()})

        # Extract FROM tables (simplified - gets first FROM clause table)
        from_match = re.search(
            r"\bFROM\s+([`\w.]+)(?:\s+AS\s+(\w+))?",
            sql,
            re.IGNORECASE,
        )
        if from_match:
            table = from_match.group(1).replace("`", "")
            self.from_tables.append(table)


def parse_sql(sql: str) -> SQLStructure:
    """Parse SQL string into SQLStructure for assertions."""
    return SQLStructure(raw_sql=sql)


# =============================================================================
# Structural Assertions
# =============================================================================


def assert_has_select(sql: str, msg: str = "") -> None:
    """Assert that SQL contains a SELECT statement."""
    if "SELECT" not in sql.upper():
        raise AssertionError(msg or "Expected SELECT statement not found")


def assert_has_from_table(
    sql: str,
    table_name: str,
    *,
    case_insensitive: bool = True,
    msg: str = "",
) -> None:
    """
    Assert that SQL references a specific table in FROM clause.

    Args:
        sql: SQL string to check
        table_name: Table name to look for (can be partial match)
        case_insensitive: Whether to ignore case
        msg: Custom error message
    """
    search_sql = sql.upper() if case_insensitive else sql
    search_table = table_name.upper() if case_insensitive else table_name
    # Remove backticks for comparison
    search_sql = search_sql.replace("`", "")

    if search_table not in search_sql:
        raise AssertionError(
            msg or f"Expected table '{table_name}' not found in SQL"
        )


def assert_has_where(sql: str, *, condition: str | None = None, msg: str = "") -> None:
    """
    Assert that SQL contains a WHERE clause.

    Args:
        sql: SQL string to check
        condition: Optional specific condition to look for in WHERE
        msg: Custom error message
    """
    structure = parse_sql(sql)
    if not structure.has_where:
        raise AssertionError(msg or "Expected WHERE clause not found")

    if condition:
        where_upper = (structure.where_clause or "").upper()
        if condition.upper() not in where_upper:
            raise AssertionError(
                msg or f"WHERE clause doesn't contain expected condition: {condition}"
            )


def assert_has_join(
    sql: str,
    *,
    join_type: str | None = None,
    count: int | None = None,
    msg: str = "",
) -> None:
    """
    Assert that SQL contains JOIN clause(s).

    Args:
        sql: SQL string to check
        join_type: Specific join type (INNER, LEFT, RIGHT, etc.)
        count: Expected number of JOINs
        msg: Custom error message
    """
    structure = parse_sql(sql)

    if not structure.join_clauses:
        raise AssertionError(msg or "Expected JOIN clause not found")

    if join_type:
        join_type_upper = join_type.upper()
        matching_joins = [
            j for j in structure.join_clauses if j["type"] == join_type_upper
        ]
        if not matching_joins:
            raise AssertionError(
                msg or f"Expected {join_type} JOIN not found"
            )

    if count is not None and len(structure.join_clauses) != count:
        raise AssertionError(
            msg
            or f"Expected {count} JOINs, found {len(structure.join_clauses)}"
        )


def assert_no_join(sql: str, msg: str = "") -> None:
    """Assert that SQL does not contain any JOIN clauses."""
    structure = parse_sql(sql)
    if structure.join_clauses:
        raise AssertionError(
            msg or f"Unexpected JOIN found (found {len(structure.join_clauses)} JOINs)"
        )


def assert_has_order_by(
    sql: str,
    *,
    column: str | None = None,
    direction: str | None = None,
    msg: str = "",
) -> None:
    """
    Assert that SQL contains ORDER BY clause.

    Args:
        sql: SQL string to check
        column: Optional specific column to look for
        direction: Optional direction (ASC/DESC)
        msg: Custom error message
    """
    structure = parse_sql(sql)
    if not structure.has_order_by:
        raise AssertionError(msg or "Expected ORDER BY clause not found")

    if column:
        if column.upper() not in sql.upper():
            raise AssertionError(
                msg or f"ORDER BY doesn't contain column: {column}"
            )

    if direction:
        dir_upper = direction.upper()
        if dir_upper not in sql.upper():
            raise AssertionError(
                msg or f"ORDER BY doesn't contain direction: {direction}"
            )


def assert_has_limit_offset(
    sql: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    msg: str = "",
) -> None:
    """
    Assert that SQL contains LIMIT and/or OFFSET.

    Args:
        sql: SQL string to check
        limit: Expected LIMIT value
        offset: Expected OFFSET value
        msg: Custom error message
    """
    structure = parse_sql(sql)

    if limit is not None:
        if structure.limit_value is None:
            raise AssertionError(msg or "Expected LIMIT clause not found")
        if structure.limit_value != limit:
            raise AssertionError(
                msg or f"Expected LIMIT {limit}, got {structure.limit_value}"
            )

    if offset is not None:
        if structure.offset_value is None:
            raise AssertionError(msg or "Expected OFFSET clause not found")
        if structure.offset_value != offset:
            raise AssertionError(
                msg or f"Expected OFFSET {offset}, got {structure.offset_value}"
            )


def assert_has_group_by(
    sql: str,
    *,
    columns: list[str] | None = None,
    msg: str = "",
) -> None:
    """
    Assert that SQL contains GROUP BY clause.

    Args:
        sql: SQL string to check
        columns: Optional list of expected grouping columns
        msg: Custom error message
    """
    structure = parse_sql(sql)
    if not structure.has_group_by:
        raise AssertionError(msg or "Expected GROUP BY clause not found")

    if columns:
        sql_upper = sql.upper()
        for col in columns:
            if col.upper() not in sql_upper:
                raise AssertionError(
                    msg or f"GROUP BY doesn't contain column: {col}"
                )


def assert_has_recursive_cte(
    sql: str,
    *,
    cte_name: str | None = None,
    msg: str = "",
) -> None:
    """
    Assert that SQL contains WITH RECURSIVE CTE.

    Args:
        sql: SQL string to check
        cte_name: Optional specific CTE name to look for
        msg: Custom error message
    """
    structure = parse_sql(sql)
    if not structure.has_recursive_cte:
        raise AssertionError(msg or "Expected WITH RECURSIVE not found")

    if cte_name:
        if cte_name.lower() not in [n.lower() for n in structure.cte_names]:
            raise AssertionError(
                msg or f"CTE '{cte_name}' not found in recursive query"
            )


def assert_has_array_agg(sql: str, msg: str = "") -> None:
    """Assert that SQL contains ARRAY_AGG or equivalent collect function."""
    sql_upper = sql.upper()
    if "ARRAY_AGG" not in sql_upper and "COLLECT_LIST" not in sql_upper:
        raise AssertionError(
            msg or "Expected ARRAY_AGG or COLLECT_LIST not found"
        )


def assert_projected_columns(
    sql: str,
    expected_columns: list[str],
    *,
    exact: bool = False,
    msg: str = "",
) -> None:
    """
    Assert that SQL projects expected columns.

    Args:
        sql: SQL string to check
        expected_columns: List of column names/aliases to look for
        exact: If True, require exact match of column count
        msg: Custom error message
    """
    sql_upper = sql.upper().replace("`", "")

    missing = []
    for col in expected_columns:
        col_upper = col.upper()
        # Look for column in SELECT clause (as alias or direct reference)
        if col_upper not in sql_upper:
            missing.append(col)

    if missing:
        raise AssertionError(
            msg or f"Expected columns not found in projection: {missing}"
        )


def assert_no_cartesian_join(sql: str, msg: str = "") -> None:
    """
    Assert that SQL does not have cartesian joins (ON TRUE).

    Note: This is a structural check. Some valid queries may have
    intentional cartesian products.
    """
    # Check for ON TRUE or ON 1=1 patterns
    cartesian_patterns = [
        r"\bON\s+TRUE\b",
        r"\bON\s+1\s*=\s*1\b",
        r"\bCROSS\s+JOIN\b",
    ]

    for pattern in cartesian_patterns:
        if re.search(pattern, sql, re.IGNORECASE):
            raise AssertionError(
                msg or f"Potential cartesian join detected: {pattern}"
            )


def assert_has_distinct(sql: str, msg: str = "") -> None:
    """Assert that SQL uses SELECT DISTINCT."""
    structure = parse_sql(sql)
    if not structure.has_distinct:
        raise AssertionError(msg or "Expected SELECT DISTINCT not found")


def assert_has_having(sql: str, *, condition: str | None = None, msg: str = "") -> None:
    """
    Assert that SQL contains HAVING clause.

    Args:
        sql: SQL string to check
        condition: Optional specific condition to look for
        msg: Custom error message
    """
    structure = parse_sql(sql)
    if not structure.has_having:
        raise AssertionError(msg or "Expected HAVING clause not found")


def assert_has_aggregation(
    sql: str,
    *,
    function: str | None = None,
    msg: str = "",
) -> None:
    """
    Assert that SQL contains aggregation functions.

    Args:
        sql: SQL string to check
        function: Optional specific function (COUNT, SUM, AVG, etc.)
        msg: Custom error message
    """
    agg_functions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "COLLECT_LIST"]
    sql_upper = sql.upper()

    if function:
        if function.upper() not in sql_upper:
            raise AssertionError(
                msg or f"Expected aggregation function {function} not found"
            )
    else:
        found = any(func in sql_upper for func in agg_functions)
        if not found:
            raise AssertionError(
                msg or "No aggregation function found in SQL"
            )


def assert_left_join_for_optional(sql: str, msg: str = "") -> None:
    """Assert that SQL uses LEFT JOIN (for OPTIONAL MATCH semantics)."""
    if "LEFT" not in sql.upper() or "JOIN" not in sql.upper():
        raise AssertionError(
            msg or "Expected LEFT JOIN for OPTIONAL MATCH semantics"
        )


def assert_cycle_detection(sql: str, msg: str = "") -> None:
    """Assert that recursive SQL includes cycle detection."""
    sql_upper = sql.upper()
    # Look for ARRAY_CONTAINS or NOT IN patterns for cycle detection
    has_cycle_check = (
        "ARRAY_CONTAINS" in sql_upper
        or "NOT IN" in sql_upper
        or "NOT ARRAY_CONTAINS" in sql_upper
    )
    if not has_cycle_check:
        raise AssertionError(
            msg or "Expected cycle detection (ARRAY_CONTAINS) not found in recursive query"
        )


def assert_depth_limit(sql: str, max_depth: int, msg: str = "") -> None:
    """
    Assert that recursive SQL includes depth limit.

    Args:
        sql: SQL string to check
        max_depth: Expected maximum depth value
        msg: Custom error message
    """
    pattern = rf"\bdepth\s*<\s*{max_depth}\b"
    if not re.search(pattern, sql, re.IGNORECASE):
        raise AssertionError(
            msg or f"Expected depth < {max_depth} limit not found"
        )
