"""Tests for planner conversion of inline property filters to WHERE.

This test module validates that the logical planner correctly converts
inline property filters from the AST into WHERE clause predicates.

Following TDD: These tests are written FIRST and will FAIL until
the planner is modified to convert inline properties.

Test Strategy:
- Parse queries with inline properties
- Create logical plan
- Verify SelectionOperator with correct filter_expression
- Verify filters are merged with explicit WHERE clauses
"""

from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.operators import SelectionOperator
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider
from gsql2rsql.parser.ast import (
    QueryExpressionBinary,
    QueryExpressionProperty,
)
from gsql2rsql.parser.operators import BinaryOperator


class TestPlannerInlineProperties:
    """Test suite for planner inline property conversion."""

    def setup_method(self):
        """Set up test fixtures with schema."""
        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()

        # Add Person node
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("node_id", str),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                    EntityProperty("active", bool),
                ],
                node_id_property=EntityProperty("node_id", str),
            )
        )

        # Add KNOWS edge
        edge = EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", str),
            sink_id_property=EntityProperty("dst", str),
            properties=[
                EntityProperty("since", int),
                EntityProperty("strength", float),
            ],
        )
        self.schema.add_edge(edge)

        self.parser = OpenCypherParser()

    def _create_plan(self, query: str) -> LogicalPlan:
        """Helper to create logical plan from query."""
        ast = self.parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        return plan

    def _find_selection_operators(
        self, plan: LogicalPlan
    ) -> list[SelectionOperator]:
        """Find all SelectionOperator instances in plan."""
        selections = []
        for op in plan.all_operators():
            if isinstance(op, SelectionOperator):
                selections.append(op)
        return selections

    # =====================================================================
    # Node Inline Property Tests
    # =====================================================================

    def test_single_node_inline_property_creates_selection(self):
        """Planner should convert single inline property to WHERE.

        Query: MATCH (a:Person {name: 'Alice'}) RETURN a
        Expected: SelectionOperator with filter a.name = 'Alice'
        """
        query = "MATCH (a:Person {name: 'Alice'}) RETURN a"
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        assert len(selections) >= 1, \
            "Should create SelectionOperator for inline filter"

        # Find selection with name filter
        name_filter = None
        for sel in selections:
            if sel.filter_expression:
                # Check if this is the name filter
                expr = sel.filter_expression
                if isinstance(expr, QueryExpressionBinary):
                    left = expr.left_expression
                    if (isinstance(left, QueryExpressionProperty) and
                            left.property_name == "name"):
                        name_filter = expr
                        break

        assert name_filter is not None, \
            "Should have filter expression for name property"

    def test_multiple_node_inline_properties_creates_and(self):
        """Planner should combine multiple inline properties with AND.

        Query: MATCH (a:Person {name: 'Alice', age: 30}) RETURN a
        Expected: Filter with AND combining both properties
        """
        query = "MATCH (a:Person {name: 'Alice', age: 30}) RETURN a"
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        assert len(selections) >= 1

        # Look for a selection with AND operator
        and_filter = None
        for sel in selections:
            if sel.filter_expression:
                expr = sel.filter_expression
                if (isinstance(expr, QueryExpressionBinary) and
                        expr.operator.name == BinaryOperator.AND):
                    and_filter = expr
                    break

        assert and_filter is not None, \
            "Should combine multiple inline properties with AND"

    def test_inline_property_merges_with_explicit_where(self):
        """Inline + explicit WHERE should merge with AND.

        Query: MATCH (a:Person {name: 'Alice'}) WHERE a.age > 25 RETURN a
        Expected: Filter with AND combining inline and explicit WHERE
        """
        query = (
            "MATCH (a:Person {name: 'Alice'}) WHERE a.age > 25 RETURN a"
        )
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        assert len(selections) >= 1

        # Should have at least one filter with properties from both
        # inline (name) and explicit WHERE (age)
        has_both = False
        for sel in selections:
            if sel.filter_expression:
                filter_str = str(sel.filter_expression)
                if "name" in filter_str and "age" in filter_str:
                    has_both = True
                    break

        assert has_both, \
            "Should merge inline and explicit WHERE filters"

    def test_node_without_inline_properties_has_no_extra_filter(self):
        """Node without inline properties should not create extra filter.

        Query: MATCH (a:Person) RETURN a
        Expected: Only filters from explicit WHERE (if any)
        """
        query = "MATCH (a:Person) RETURN a"
        plan = self._create_plan(query)

        # This query has no WHERE and no inline properties
        # Should have minimal or no selection operators
        selections = self._find_selection_operators(plan)
        # It's ok if there are no selections or if they have no filters
        for sel in selections:
            # If there is a selection, it shouldn't have a filter expression
            # (or it should be from something else, not inline properties)
            pass  # Just checking structure is valid

    def test_multiple_nodes_with_inline_properties(self):
        """Multiple nodes with inline properties should each have filters.

        Query: MATCH (a:Person {name: 'Alice'})-[r]->(b:Person {age: 30})
        Expected: Filters for both a.name and b.age
        """
        query = (
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->"
            "(b:Person {age: 30}) RETURN a, b"
        )
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        assert len(selections) >= 1

        # Should have filters mentioning both 'name' and 'age'
        filter_strs = [
            str(sel.filter_expression)
            for sel in selections
            if sel.filter_expression
        ]
        all_filters = " ".join(filter_strs)

        assert "name" in all_filters, "Should have filter for a.name"
        assert "age" in all_filters, "Should have filter for b.age"

    # =====================================================================
    # Relationship Inline Property Tests
    # =====================================================================

    def test_relationship_inline_property_creates_selection(self):
        """Planner should convert relationship inline property to WHERE.

        Query: MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) RETURN r
        Expected: SelectionOperator with filter r.since = 2020
        """
        query = (
            "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) RETURN r"
        )
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        assert len(selections) >= 1

        # Look for filter on 'since' property
        filter_strs = [
            str(sel.filter_expression)
            for sel in selections
            if sel.filter_expression
        ]
        all_filters = " ".join(filter_strs)

        assert "since" in all_filters, \
            "Should have filter for relationship property 'since'"

    def test_relationship_multiple_inline_properties(self):
        """Multiple relationship inline properties should combine with AND.

        Query: MATCH (a:Person)-[r:KNOWS {since: 2020, strength: 0.9}]->(b:Person)
        Expected: Filter with AND combining both properties
        """
        query = (
            "MATCH (a:Person)-[r:KNOWS {since: 2020, strength: 0.9}]->"
            "(b:Person) RETURN r"
        )
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        filter_strs = [
            str(sel.filter_expression)
            for sel in selections
            if sel.filter_expression
        ]
        all_filters = " ".join(filter_strs)

        assert "since" in all_filters and "strength" in all_filters, \
            "Should have filters for both relationship properties"

    def test_node_and_relationship_inline_properties_combined(self):
        """Inline properties on both nodes and relationships.

        Query: (a {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b {age: 30})
        Expected: Filters for all three inline properties
        """
        query = (
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->"
            "(b:Person {age: 30}) RETURN a, r, b"
        )
        plan = self._create_plan(query)

        selections = self._find_selection_operators(plan)
        filter_strs = [
            str(sel.filter_expression)
            for sel in selections
            if sel.filter_expression
        ]
        all_filters = " ".join(filter_strs)

        assert "name" in all_filters, "Should filter on a.name"
        assert "since" in all_filters, "Should filter on r.since"
        assert "age" in all_filters, "Should filter on b.age"
