"""Unit tests for extract_source_node_filter and AND-tree helpers.

Tests various AND-tree shapes:
- Single condition (no AND)
- Two conditions (simple AND)
- Three+ conditions (nested left-associative AND — the bug case)
- Right-associative AND trees
- Deeply nested (5+ conjuncts)
- Mixed: some source-only, some not
- All source-only conditions
- No source conditions at all
- None input
- OR expressions (should NOT be flattened)
- Non-property expressions (literals with no variable refs)
"""

from __future__ import annotations

import pytest

from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionProperty,
    QueryExpressionValue,
    QueryExpressionFunction,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
)
from gsql2rsql.planner.recursive_traversal import (
    _flatten_and_tree,
    _rebuild_and_tree,
    extract_source_node_filter,
)


# ---------------------------------------------------------------------------
# Helpers to build AST nodes concisely
# ---------------------------------------------------------------------------

AND_OP = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
OR_OP = BinaryOperatorInfo(BinaryOperator.OR, BinaryOperatorType.LOGICAL)
EQ_OP = BinaryOperatorInfo(BinaryOperator.EQ, BinaryOperatorType.COMPARISON)
GT_OP = BinaryOperatorInfo(BinaryOperator.GT, BinaryOperatorType.COMPARISON)


def prop(var: str, name: str) -> QueryExpressionProperty:
    return QueryExpressionProperty(variable_name=var, property_name=name)


def lit_str(val: str) -> QueryExpressionValue:
    return QueryExpressionValue(value=val, value_type=str)


def lit_bool(val: bool) -> QueryExpressionValue:
    return QueryExpressionValue(value=val, value_type=bool)


def lit_int(val: int) -> QueryExpressionValue:
    return QueryExpressionValue(value=val, value_type=int)


def eq(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=EQ_OP, left_expression=left, right_expression=right)


def gt(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=GT_OP, left_expression=left, right_expression=right)


def and_(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=AND_OP, left_expression=left, right_expression=right)


def or_(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=OR_OP, left_expression=left, right_expression=right)


def not_fn(inner: QueryExpression) -> QueryExpressionFunction:
    return QueryExpressionFunction(function=Function.NOT, parameters=[inner], data_type=bool)


def left_assoc_and(parts: list[QueryExpression]) -> QueryExpression:
    """Build left-associative AND tree: ((A AND B) AND C) AND D ..."""
    assert len(parts) >= 2
    result = parts[0]
    for p in parts[1:]:
        result = and_(result, p)
    return result


def right_assoc_and(parts: list[QueryExpression]) -> QueryExpression:
    """Build right-associative AND tree: A AND (B AND (C AND D))."""
    assert len(parts) >= 2
    result = parts[-1]
    for p in reversed(parts[:-1]):
        result = and_(p, result)
    return result


# Reusable leaf expressions
A_ID = eq(prop("a", "node_id"), lit_str("N1"))       # references 'a'
A_NAME = eq(prop("a", "name"), lit_str("Alice"))      # references 'a'
A_AGE = gt(prop("a", "age"), lit_int(30))             # references 'a'
B_HUB = eq(prop("b", "is_hub"), lit_bool(True))       # references 'b'
B_TYPE = eq(prop("b", "type"), lit_str("Station"))     # references 'b'
C_WEIGHT = gt(prop("c", "weight"), lit_int(100))       # references 'c'
NOT_R = not_fn(prop("r", "src_is_hub"))                # references 'r' (via children)


# ===================================================================
# Tests for _flatten_and_tree
# ===================================================================

class TestFlattenAndTree:
    def test_single_non_and(self):
        """A single expression (not AND) returns a 1-element list."""
        result = _flatten_and_tree(A_ID)
        assert result == [A_ID]

    def test_simple_and(self):
        """A AND B -> [A, B]."""
        expr = and_(A_ID, B_HUB)
        result = _flatten_and_tree(expr)
        assert result == [A_ID, B_HUB]

    def test_left_associative_3(self):
        """(A AND B) AND C -> [A, B, C]."""
        expr = and_(and_(A_ID, B_HUB), C_WEIGHT)
        result = _flatten_and_tree(expr)
        assert result == [A_ID, B_HUB, C_WEIGHT]

    def test_right_associative_3(self):
        """A AND (B AND C) -> [A, B, C]."""
        expr = and_(A_ID, and_(B_HUB, C_WEIGHT))
        result = _flatten_and_tree(expr)
        assert result == [A_ID, B_HUB, C_WEIGHT]

    def test_left_associative_5(self):
        """((((A AND B) AND C) AND D) AND E) -> [A, B, C, D, E]."""
        parts = [A_ID, B_HUB, C_WEIGHT, A_NAME, B_TYPE]
        expr = left_assoc_and(parts)
        result = _flatten_and_tree(expr)
        assert result == parts

    def test_right_associative_5(self):
        """A AND (B AND (C AND (D AND E))) -> [A, B, C, D, E]."""
        parts = [A_ID, B_HUB, C_WEIGHT, A_NAME, B_TYPE]
        expr = right_assoc_and(parts)
        result = _flatten_and_tree(expr)
        assert result == parts

    def test_mixed_associativity(self):
        """(A AND B) AND (C AND D) -> [A, B, C, D]."""
        left = and_(A_ID, B_HUB)
        right = and_(C_WEIGHT, A_NAME)
        expr = and_(left, right)
        result = _flatten_and_tree(expr)
        assert result == [A_ID, B_HUB, C_WEIGHT, A_NAME]

    def test_or_not_flattened(self):
        """OR should NOT be flattened — returned as single element."""
        expr = or_(A_ID, B_HUB)
        result = _flatten_and_tree(expr)
        assert len(result) == 1
        assert result[0] is expr

    def test_and_containing_or(self):
        """(A OR B) AND C -> [(A OR B), C]. OR subtree stays intact."""
        or_expr = or_(A_ID, B_HUB)
        expr = and_(or_expr, C_WEIGHT)
        result = _flatten_and_tree(expr)
        assert len(result) == 2
        assert result[0] is or_expr
        assert result[1] is C_WEIGHT


# ===================================================================
# Tests for _rebuild_and_tree
# ===================================================================

class TestRebuildAndTree:
    def test_single_element(self):
        """Single element returns itself (no wrapping)."""
        result = _rebuild_and_tree([A_ID])
        assert result is A_ID

    def test_two_elements(self):
        """[A, B] -> A AND B."""
        result = _rebuild_and_tree([A_ID, B_HUB])
        assert isinstance(result, QueryExpressionBinary)
        assert result.operator.name == BinaryOperator.AND
        assert result.left_expression is A_ID
        assert result.right_expression is B_HUB

    def test_three_elements_left_associative(self):
        """[A, B, C] -> (A AND B) AND C."""
        result = _rebuild_and_tree([A_ID, B_HUB, C_WEIGHT])
        assert isinstance(result, QueryExpressionBinary)
        assert result.right_expression is C_WEIGHT
        inner = result.left_expression
        assert isinstance(inner, QueryExpressionBinary)
        assert inner.left_expression is A_ID
        assert inner.right_expression is B_HUB

    def test_empty_list_raises(self):
        """Empty list should raise AssertionError."""
        with pytest.raises(AssertionError, match="Cannot rebuild AND tree from empty list"):
            _rebuild_and_tree([])

    def test_roundtrip_preserves_conjuncts(self):
        """flatten then rebuild preserves all conjuncts."""
        parts = [A_ID, B_HUB, C_WEIGHT, A_NAME, B_TYPE]
        tree = left_assoc_and(parts)
        flattened = _flatten_and_tree(tree)
        rebuilt = _rebuild_and_tree(flattened)
        re_flattened = _flatten_and_tree(rebuilt)
        assert re_flattened == parts


# ===================================================================
# Tests for extract_source_node_filter
# ===================================================================

class TestExtractSourceNodeFilter:
    """Tests for the main extraction function with various tree shapes."""

    def test_none_input(self):
        """None -> (None, None)."""
        src, rem = extract_source_node_filter(None, "a")
        assert src is None
        assert rem is None

    def test_single_source_only(self):
        """a.node_id = 'N1' -> (a.node_id = 'N1', None)."""
        src, rem = extract_source_node_filter(A_ID, "a")
        assert src is A_ID
        assert rem is None

    def test_single_non_source(self):
        """b.is_hub = true -> (None, b.is_hub = true)."""
        src, rem = extract_source_node_filter(B_HUB, "a")
        assert src is None
        assert rem is B_HUB

    def test_two_conditions_one_source(self):
        """a.node_id = 'N1' AND b.is_hub = true -> (a.node_id, b.is_hub)."""
        expr = and_(A_ID, B_HUB)
        src, rem = extract_source_node_filter(expr, "a")
        assert str(src) == str(A_ID)
        assert str(rem) == str(B_HUB)

    def test_two_conditions_both_source(self):
        """a.node_id = 'N1' AND a.name = 'Alice' -> (both, None)."""
        expr = and_(A_ID, A_NAME)
        src, rem = extract_source_node_filter(expr, "a")
        # Both reference 'a', so extracted is the full AND, remaining is None
        assert rem is None
        assert src is not None
        flat = _flatten_and_tree(src)
        assert len(flat) == 2

    def test_two_conditions_neither_source(self):
        """b.is_hub AND c.weight > 100 -> (None, original)."""
        expr = and_(B_HUB, C_WEIGHT)
        src, rem = extract_source_node_filter(expr, "a")
        assert src is None
        assert rem is expr

    # ---- THE BUG CASE: 3 conditions, left-associative ----

    def test_three_conditions_left_assoc_source_first(self):
        """((a.id='N1' AND b.hub=true) AND c.weight>100).

        Previously failed: left subtree mixes a+b, so extraction returned (None, full_expr).
        After fix: correctly extracts a.id='N1'.
        """
        expr = left_assoc_and([A_ID, B_HUB, C_WEIGHT])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None, "Source filter must be extracted from nested AND"
        assert str(src) == str(A_ID)
        # remaining should be B_HUB AND C_WEIGHT
        assert rem is not None
        rem_parts = _flatten_and_tree(rem)
        assert len(rem_parts) == 2
        assert str(rem_parts[0]) == str(B_HUB)
        assert str(rem_parts[1]) == str(C_WEIGHT)

    def test_three_conditions_left_assoc_source_middle(self):
        """((b.hub AND a.id) AND c.weight) -> extracts a.id."""
        expr = left_assoc_and([B_HUB, A_ID, C_WEIGHT])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        assert str(src) == str(A_ID)
        rem_parts = _flatten_and_tree(rem)
        assert len(rem_parts) == 2

    def test_three_conditions_left_assoc_source_last(self):
        """((b.hub AND c.weight) AND a.id) -> extracts a.id."""
        expr = left_assoc_and([B_HUB, C_WEIGHT, A_ID])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        assert str(src) == str(A_ID)
        rem_parts = _flatten_and_tree(rem)
        assert len(rem_parts) == 2

    def test_three_conditions_right_assoc(self):
        """a.id AND (b.hub AND c.weight) -> extracts a.id."""
        expr = right_assoc_and([A_ID, B_HUB, C_WEIGHT])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        assert str(src) == str(A_ID)

    def test_multiple_source_conditions_in_nested_and(self):
        """((a.id AND b.hub) AND a.name) -> extracts (a.id AND a.name)."""
        expr = left_assoc_and([A_ID, B_HUB, A_NAME])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        src_parts = _flatten_and_tree(src)
        assert len(src_parts) == 2
        assert str(src_parts[0]) == str(A_ID)
        assert str(src_parts[1]) == str(A_NAME)
        assert rem is not None
        assert str(rem) == str(B_HUB)

    def test_five_conditions_mixed(self):
        """((((a.id AND b.hub) AND c.weight) AND a.name) AND b.type).

        Should extract a.id AND a.name; remaining b.hub AND c.weight AND b.type.
        """
        expr = left_assoc_and([A_ID, B_HUB, C_WEIGHT, A_NAME, B_TYPE])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        src_parts = _flatten_and_tree(src)
        assert len(src_parts) == 2
        assert str(src_parts[0]) == str(A_ID)
        assert str(src_parts[1]) == str(A_NAME)

        rem_parts = _flatten_and_tree(rem)
        assert len(rem_parts) == 3
        assert str(rem_parts[0]) == str(B_HUB)
        assert str(rem_parts[1]) == str(C_WEIGHT)
        assert str(rem_parts[2]) == str(B_TYPE)

    def test_all_source_in_nested_and(self):
        """((a.id AND a.name) AND a.age) -> entire expression extracted."""
        expr = left_assoc_and([A_ID, A_NAME, A_AGE])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        assert rem is None
        src_parts = _flatten_and_tree(src)
        assert len(src_parts) == 3

    def test_no_source_in_nested_and(self):
        """((b.hub AND c.weight) AND b.type) -> (None, original)."""
        expr = left_assoc_and([B_HUB, C_WEIGHT, B_TYPE])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is None
        assert rem is expr

    def test_or_expression_not_decomposed(self):
        """a.id OR b.hub -> (None, original). OR should not be split."""
        expr = or_(A_ID, B_HUB)
        src, rem = extract_source_node_filter(expr, "a")
        assert src is None
        assert rem is expr

    def test_and_with_or_subtree(self):
        """a.id AND (b.hub OR c.weight) -> extracts a.id, keeps OR subtree."""
        or_subtree = or_(B_HUB, C_WEIGHT)
        expr = and_(A_ID, or_subtree)
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        assert str(src) == str(A_ID)
        assert rem is or_subtree

    def test_nested_and_with_or_subtree(self):
        """((a.id AND (b.hub OR c.weight)) AND a.name).

        Should extract a.id AND a.name; remaining is the OR subtree.
        """
        or_subtree = or_(B_HUB, C_WEIGHT)
        expr = left_assoc_and([A_ID, or_subtree, A_NAME])
        src, rem = extract_source_node_filter(expr, "a")
        assert src is not None
        src_parts = _flatten_and_tree(src)
        assert len(src_parts) == 2
        assert str(src_parts[0]) == str(A_ID)
        assert str(src_parts[1]) == str(A_NAME)
        assert rem is or_subtree

    def test_literal_only_expression(self):
        """1 = 1 has no variable references -> (None, expr).

        _references_only_variable returns False for empty property lists.
        """
        expr = eq(lit_int(1), lit_int(1))
        src, rem = extract_source_node_filter(expr, "a")
        assert src is None
        assert rem is expr

    def test_not_function_child_references(self):
        """NOT(r.src_is_hub) references 'r' via children traversal.

        When checking variable='r', should be extracted.
        When checking variable='a', should NOT be extracted.
        """
        src_r, rem_r = extract_source_node_filter(NOT_R, "r")
        assert src_r is NOT_R
        assert rem_r is None

        src_a, rem_a = extract_source_node_filter(NOT_R, "a")
        assert src_a is None
        assert rem_a is NOT_R


class TestExtractWithParsedCypher:
    """Integration-style tests using the actual parser for realistic ASTs."""

    @pytest.fixture
    def parser(self):
        from gsql2rsql.parser.opencypher_parser import OpenCypherParser
        return OpenCypherParser()

    def _get_where(self, parser, cypher: str) -> QueryExpression:
        ast = parser.parse(cypher)
        return ast.parts[0].match_clauses[0].where_expression

    def test_parsed_three_conditions(self, parser):
        """Real parser produces left-associative: ((a.id AND b.hub) AND ALL(...))."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        where = self._get_where(parser, cypher)
        src, rem = extract_source_node_filter(where, "a")
        assert src is not None, "Parser-produced nested AND must extract a.node_id='N1'"
        assert "node_id" in str(src)
        assert "N1" in str(src)

        rem_parts = _flatten_and_tree(rem)
        assert len(rem_parts) == 2

    def test_parsed_two_conditions(self, parser):
        """Simple case: a.id AND b.hub (no nesting issue)."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1' AND b.is_hub = true
        RETURN b.node_id AS dst
        """
        where = self._get_where(parser, cypher)
        src, rem = extract_source_node_filter(where, "a")
        assert src is not None
        assert "N1" in str(src)
        assert rem is not None
        assert "is_hub" in str(rem)

    def test_parsed_four_conditions(self, parser):
        """4 AND conditions: a.id AND b.hub AND a.name AND c.type."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND a.node_type = 'main'
          AND b.node_type = 'Station'
        RETURN b.node_id AS dst
        """
        where = self._get_where(parser, cypher)
        src, rem = extract_source_node_filter(where, "a")
        assert src is not None
        src_parts = _flatten_and_tree(src)
        assert len(src_parts) == 2  # a.node_id and a.node_type
        rem_parts = _flatten_and_tree(rem)
        assert len(rem_parts) == 2  # b.is_hub and b.node_type

    def test_parsed_single_source(self, parser):
        """Only source condition -> (source, None)."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
        RETURN b.node_id AS dst
        """
        where = self._get_where(parser, cypher)
        src, rem = extract_source_node_filter(where, "a")
        assert src is not None
        assert rem is None
