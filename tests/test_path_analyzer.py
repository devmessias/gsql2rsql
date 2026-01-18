"""Tests for PathExpressionAnalyzer.

These tests verify that the path analyzer correctly:
1. Detects when edge collection is needed (relationships(path) usage)
2. Detects when node collection is needed (nodes(path) usage)
3. Extracts edge predicates for potential pushdown
4. Handles nested expressions correctly

The analyzer is critical for CTE optimization - it determines whether
to collect edge properties (expensive) or skip them (cheap).
"""

import pytest

from gsql2rsql.parser.ast import (
    ListPredicateType,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionListPredicate,
    QueryExpressionListComprehension,
    QueryExpressionProperty,
    QueryExpressionReduce,
    QueryExpressionValue,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
)
from gsql2rsql.planner.path_analyzer import (
    PathExpressionAnalyzer,
    PathUsageInfo,
    rewrite_predicate_for_edge_alias,
)


class TestPathExpressionAnalyzer:
    """Tests for PathExpressionAnalyzer class."""

    @pytest.fixture
    def analyzer(self) -> PathExpressionAnalyzer:
        """Create analyzer instance for tests."""
        return PathExpressionAnalyzer()

    def test_no_path_variable_no_collection(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """When path variable is empty, no collection is needed."""
        info = analyzer.analyze(
            path_variable="",
            where_expr=None,
            return_exprs=None,
        )

        assert info.needs_edge_collection is False
        assert info.needs_node_collection is False
        assert len(info.edge_predicates) == 0

    def test_size_path_no_edge_collection(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """SIZE(path) does not require edge collection.

        SIZE(path) only uses the length of the path array,
        not the edge properties.
        """
        # SIZE(path) - function call with path as parameter
        size_expr = QueryExpressionFunction(
            function=Function.SIZE,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=None,
            return_exprs=[size_expr],
        )

        # SIZE uses the path array, not relationships
        assert info.needs_edge_collection is False

    def test_nodes_path_needs_node_collection(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """nodes(path) requires node collection.

        Currently nodes are always in the path array, so this is
        informational. But the analyzer should detect the usage.
        """
        # nodes(path)
        nodes_expr = QueryExpressionFunction(
            function=Function.NODES,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=None,
            return_exprs=[nodes_expr],
        )

        assert info.needs_node_collection is True
        assert info.needs_edge_collection is False

    def test_relationships_path_needs_edge_collection(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """relationships(path) requires edge collection.

        When relationships(path) is used, we need to collect edge
        properties in the CTE so they can be accessed later.
        """
        # relationships(path)
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=None,
            return_exprs=[rels_expr],
        )

        assert info.needs_edge_collection is True

    def test_all_relationships_extracts_predicate(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """ALL(rel IN relationships(path) WHERE pred) extracts predicate.

        The predicate from ALL expressions can be pushed down into
        the CTE for early filtering.
        """
        # rel.amount > 1000
        pred = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="rel", property_name="amount"
            ),
            right_expression=QueryExpressionValue(value=1000, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )

        # relationships(path)
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        # ALL(rel IN relationships(path) WHERE rel.amount > 1000)
        all_expr = QueryExpressionListPredicate(
            predicate_type=ListPredicateType.ALL,
            variable_name="rel",
            list_expression=rels_expr,
            filter_expression=pred,
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=all_expr,
            return_exprs=None,
        )

        assert info.needs_edge_collection is True
        assert len(info.edge_predicates) == 1
        assert info.edge_lambda_variable == "rel"
        assert info.has_pushable_predicates is True

    def test_any_relationships_no_predicate_extraction(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """ANY(rel IN relationships(path) WHERE pred) does not extract predicate.

        ANY predicates cannot be pushed down because they only require
        ONE edge to match, not ALL edges.
        """
        pred = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="rel", property_name="flagged"
            ),
            right_expression=QueryExpressionValue(value=True, value_type=bool),
            operator=BinaryOperatorInfo(name=BinaryOperator.EQ, operator_type=BinaryOperatorType.COMPARISON),
        )

        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        # ANY(rel IN relationships(path) WHERE rel.flagged = true)
        any_expr = QueryExpressionListPredicate(
            predicate_type=ListPredicateType.ANY,
            variable_name="rel",
            list_expression=rels_expr,
            filter_expression=pred,
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=any_expr,
            return_exprs=None,
        )

        # Edge collection is needed, but predicate is NOT pushable
        assert info.needs_edge_collection is True
        assert len(info.edge_predicates) == 0  # ANY is not pushable
        assert info.has_pushable_predicates is False

    def test_reduce_relationships_needs_edge_collection(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """REDUCE over relationships(path) needs edge collection.

        REDUCE expressions cannot be pushed down (they aggregate),
        but they do require edge collection.
        """
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        # REDUCE(sum = 0, r IN relationships(path) | sum + r.amount)
        reduce_expr = QueryExpressionReduce(
            accumulator_name="sum",
            initial_value=QueryExpressionValue(value=0, value_type=int),
            variable_name="r",
            list_expression=rels_expr,
            reducer_expression=QueryExpressionBinary(
                left_expression=QueryExpressionProperty(
                    variable_name="sum", property_name=""
                ),
                right_expression=QueryExpressionProperty(
                    variable_name="r", property_name="amount"
                ),
                operator=BinaryOperatorInfo(name=BinaryOperator.PLUS, operator_type=BinaryOperatorType.VALUE),
            ),
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=None,
            return_exprs=[reduce_expr],
        )

        assert info.needs_edge_collection is True
        # REDUCE cannot be pushed down as a filter
        assert len(info.edge_predicates) == 0

    def test_list_comprehension_with_filter(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """[r IN relationships(path) WHERE filter | expr] extracts filter.

        List comprehension filters can be pushed down similar to ALL.
        """
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        # filter: r.valid = true
        filter_expr = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="r", property_name="valid"
            ),
            right_expression=QueryExpressionValue(value=True, value_type=bool),
            operator=BinaryOperatorInfo(name=BinaryOperator.EQ, operator_type=BinaryOperatorType.COMPARISON),
        )

        # [r IN relationships(path) WHERE r.valid = true | r.id]
        comp_expr = QueryExpressionListComprehension(
            variable_name="r",
            list_expression=rels_expr,
            filter_expression=filter_expr,
            map_expression=QueryExpressionProperty(
                variable_name="r", property_name="id"
            ),
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=None,
            return_exprs=[comp_expr],
        )

        assert info.needs_edge_collection is True
        assert len(info.edge_predicates) == 1
        assert info.edge_lambda_variable == "r"

    def test_nested_expression_finds_path_usage(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """Path usage nested in complex expression is detected.

        WHERE a.x > 1 AND ALL(r IN relationships(path) WHERE r.y > 2)
        should detect the relationships(path) usage even though it's
        nested inside an AND expression.
        """
        # a.x > 1
        left_pred = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="a", property_name="x"
            ),
            right_expression=QueryExpressionValue(value=1, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )

        # relationships(path)
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        # r.y > 2
        edge_pred = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="r", property_name="y"
            ),
            right_expression=QueryExpressionValue(value=2, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )

        # ALL(r IN relationships(path) WHERE r.y > 2)
        all_expr = QueryExpressionListPredicate(
            predicate_type=ListPredicateType.ALL,
            variable_name="r",
            list_expression=rels_expr,
            filter_expression=edge_pred,
        )

        # a.x > 1 AND ALL(...)
        and_expr = QueryExpressionBinary(
            left_expression=left_pred,
            right_expression=all_expr,
            operator=BinaryOperatorInfo(name=BinaryOperator.AND, operator_type=BinaryOperatorType.LOGICAL),
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=and_expr,
            return_exprs=None,
        )

        assert info.needs_edge_collection is True
        assert len(info.edge_predicates) == 1

    def test_multiple_all_predicates_combined(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """Multiple ALL predicates are combined with AND."""
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="path", property_name="")
            ],
        )

        # ALL(r IN relationships(path) WHERE r.amount > 1000)
        pred1 = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="r", property_name="amount"
            ),
            right_expression=QueryExpressionValue(value=1000, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )
        all1 = QueryExpressionListPredicate(
            predicate_type=ListPredicateType.ALL,
            variable_name="r",
            list_expression=rels_expr,
            filter_expression=pred1,
        )

        # ALL(r IN relationships(path) WHERE r.valid = true)
        pred2 = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="r", property_name="valid"
            ),
            right_expression=QueryExpressionValue(value=True, value_type=bool),
            operator=BinaryOperatorInfo(name=BinaryOperator.EQ, operator_type=BinaryOperatorType.COMPARISON),
        )
        all2 = QueryExpressionListPredicate(
            predicate_type=ListPredicateType.ALL,
            variable_name="r",
            list_expression=rels_expr,
            filter_expression=pred2,
        )

        # ALL(...) AND ALL(...)
        and_expr = QueryExpressionBinary(
            left_expression=all1,
            right_expression=all2,
            operator=BinaryOperatorInfo(name=BinaryOperator.AND, operator_type=BinaryOperatorType.LOGICAL),
        )

        info = analyzer.analyze(
            path_variable="path",
            where_expr=and_expr,
            return_exprs=None,
        )

        assert info.needs_edge_collection is True
        assert len(info.edge_predicates) == 2
        assert info.has_pushable_predicates is True

        # Test combined predicate
        combined = info.combined_edge_predicate
        assert combined is not None
        assert isinstance(combined, QueryExpressionBinary)

    def test_different_path_variable_not_matched(
        self, analyzer: PathExpressionAnalyzer
    ) -> None:
        """relationships(other_path) doesn't trigger collection for 'path'."""
        # relationships(other_path) - different variable
        rels_expr = QueryExpressionFunction(
            function=Function.RELATIONSHIPS,
            parameters=[
                QueryExpressionProperty(variable_name="other_path", property_name="")
            ],
        )

        info = analyzer.analyze(
            path_variable="path",  # Looking for "path", not "other_path"
            where_expr=None,
            return_exprs=[rels_expr],
        )

        # Should NOT trigger collection because variable name doesn't match
        assert info.needs_edge_collection is False


class TestRewritePredicateForEdgeAlias:
    """Tests for predicate rewriting function."""

    def test_rewrite_simple_property(self) -> None:
        """rel.amount -> e.amount."""
        pred = QueryExpressionProperty(
            variable_name="rel", property_name="amount"
        )

        rewritten = rewrite_predicate_for_edge_alias(pred, "rel", "e")

        assert isinstance(rewritten, QueryExpressionProperty)
        assert rewritten.variable_name == "e"
        assert rewritten.property_name == "amount"

    def test_rewrite_binary_expression(self) -> None:
        """rel.amount > 1000 -> e.amount > 1000."""
        pred = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="rel", property_name="amount"
            ),
            right_expression=QueryExpressionValue(value=1000, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )

        rewritten = rewrite_predicate_for_edge_alias(pred, "rel", "e")

        assert isinstance(rewritten, QueryExpressionBinary)
        assert isinstance(rewritten.left_expression, QueryExpressionProperty)
        assert rewritten.left_expression.variable_name == "e"
        # Right side (constant) should be unchanged
        assert isinstance(rewritten.right_expression, QueryExpressionValue)
        assert rewritten.right_expression.value == 1000

    def test_rewrite_preserves_other_variables(self) -> None:
        """Variables other than lambda var are preserved."""
        # rel.amount > other.threshold
        pred = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="rel", property_name="amount"
            ),
            right_expression=QueryExpressionProperty(
                variable_name="other", property_name="threshold"
            ),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )

        rewritten = rewrite_predicate_for_edge_alias(pred, "rel", "e")

        assert isinstance(rewritten, QueryExpressionBinary)
        # Left side: rel -> e
        assert rewritten.left_expression.variable_name == "e"
        # Right side: other stays as other
        assert rewritten.right_expression.variable_name == "other"

    def test_rewrite_nested_and(self) -> None:
        """rel.x > 1 AND rel.y < 10 -> e.x > 1 AND e.y < 10."""
        left = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="rel", property_name="x"
            ),
            right_expression=QueryExpressionValue(value=1, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.GT, operator_type=BinaryOperatorType.COMPARISON),
        )
        right = QueryExpressionBinary(
            left_expression=QueryExpressionProperty(
                variable_name="rel", property_name="y"
            ),
            right_expression=QueryExpressionValue(value=10, value_type=int),
            operator=BinaryOperatorInfo(name=BinaryOperator.LT, operator_type=BinaryOperatorType.COMPARISON),
        )
        pred = QueryExpressionBinary(
            left_expression=left,
            right_expression=right,
            operator=BinaryOperatorInfo(name=BinaryOperator.AND, operator_type=BinaryOperatorType.LOGICAL),
        )

        rewritten = rewrite_predicate_for_edge_alias(pred, "rel", "e")

        # Both nested expressions should have rel -> e
        assert rewritten.left_expression.left_expression.variable_name == "e"
        assert rewritten.right_expression.left_expression.variable_name == "e"


class TestPathUsageInfo:
    """Tests for PathUsageInfo dataclass."""

    def test_has_pushable_predicates_empty(self) -> None:
        """No predicates -> not pushable."""
        info = PathUsageInfo(path_variable="path")
        assert info.has_pushable_predicates is False

    def test_has_pushable_predicates_with_predicates(self) -> None:
        """With predicates -> pushable."""
        info = PathUsageInfo(path_variable="path")
        info.edge_predicates.append(
            QueryExpressionValue(value=True, value_type=bool)  # Dummy predicate
        )
        assert info.has_pushable_predicates is True

    def test_combined_predicate_single(self) -> None:
        """Single predicate returns as-is."""
        pred = QueryExpressionValue(value=True, value_type=bool)
        info = PathUsageInfo(path_variable="path", edge_predicates=[pred])

        combined = info.combined_edge_predicate
        assert combined is pred

    def test_combined_predicate_multiple(self) -> None:
        """Multiple predicates combined with AND."""
        pred1 = QueryExpressionValue(value=True, value_type=bool)
        pred2 = QueryExpressionValue(value=False, value_type=bool)
        info = PathUsageInfo(
            path_variable="path", edge_predicates=[pred1, pred2]
        )

        combined = info.combined_edge_predicate
        assert isinstance(combined, QueryExpressionBinary)
        assert combined.operator.name == BinaryOperator.AND

    def test_combined_predicate_none(self) -> None:
        """No predicates returns None."""
        info = PathUsageInfo(path_variable="path")
        assert info.combined_edge_predicate is None
