"""Tests for BFS source node filtering with WHERE clause.

This test validates the fix for the bug where WHERE filters on the source node
were not being applied in variable-length path queries.

Bug: When using WHERE to filter the source node in variable-length paths,
the filter was ignored, causing the BFS to start from ALL nodes instead of
just the filtered node.

Fix: Extract source node filter and apply it in the base case of the
recursive CTE with a JOIN to the source node table.

Related issues:
- Source node filter must appear in base case (before UNION ALL)
- Must use correct node_id_property from schema (not hardcoded "id")
- Filter must NOT appear in recursive case (would cause incorrect results)
"""

import pytest

from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.renderer.sql_renderer import SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestBFSSourceNodeFilter:
    """Tests for WHERE filter on source node in variable-length paths."""

    def setup_method(self) -> None:
        """Set up test fixtures with schema."""
        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()

        # Node with custom node_id_property (not "id")
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("node_id", str),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("node_id", str),
            ),
            SQLTableDescriptor(
                table_name="test.persons",
                node_id_columns=["node_id"],
            ),
        )

        # Edge
        edge = EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", str),
            sink_id_property=EntityProperty("dst", str),
            properties=[],
        )
        self.schema.add_edge(
            edge,
            SQLTableDescriptor(
                entity_id="Person@KNOWS@Person",
                table_name="test.knows",
            ),
        )

    def _transpile(self, query: str) -> str:
        """Helper to transpile query to SQL."""
        parser = OpenCypherParser()
        renderer = SQLRenderer(db_schema_provider=self.schema)

        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=query)
        return renderer.render_plan(plan)

    def test_source_filter_applied_in_base_case(self) -> None:
        """WHERE filter on source node should appear in base case of CTE."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = "alice"
        RETURN a.node_id, b.node_id
        """

        sql = self._transpile(query)

        # Base case should have JOIN with source table
        assert "JOIN test.persons src ON src.node_id = e.src" in sql, \
            "Missing JOIN with source node table in base case"

        # WHERE clause should include the filter
        assert "(src.node_id) = ('alice')" in sql, \
            "Source node filter not applied in WHERE clause"

    def test_filter_position_in_base_case(self) -> None:
        """Filter must be in base case (before first UNION ALL)."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person)
        WHERE a.node_id = "bob"
        RETURN a.node_id, b.node_id
        """

        sql = self._transpile(query)
        lines = sql.split('\n')

        # Find key positions
        base_case_line = next(i for i, l in enumerate(lines) if "-- Base case" in l)
        union_all_line = next(i for i, l in enumerate(lines) if "UNION ALL" in l and i > base_case_line)
        filter_line = next(i for i, l in enumerate(lines) if "(src.node_id) = ('bob')" in l)

        # Filter must be between base case and first UNION ALL
        assert base_case_line < filter_line < union_all_line, \
            f"Filter not in base case: base={base_case_line}, filter={filter_line}, union={union_all_line}"

    def test_filter_not_in_recursive_case(self) -> None:
        """Filter should only appear in base case, not recursive case."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person)
        WHERE a.node_id = "charlie"
        RETURN a.node_id, b.node_id
        """

        sql = self._transpile(query)
        lines = sql.split('\n')

        # Find UNION ALL (start of recursive case)
        union_all_line = next(i for i, l in enumerate(lines) if "UNION ALL" in l)

        # Count filter occurrences in recursive case
        recursive_section = '\n'.join(lines[union_all_line:])
        filter_count = recursive_section.count("src.node_id) = ('charlie')")

        assert filter_count == 0, \
            f"Filter incorrectly appears {filter_count} times in recursive case"

    def test_uses_schema_node_id_property(self) -> None:
        """Should use node_id_property from schema, not hardcoded 'id'."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person)
        WHERE a.node_id = "dave"
        RETURN a.node_id
        """

        sql = self._transpile(query)

        # Should use node_id (from schema), not id
        assert "src.node_id = e.src" in sql, \
            "JOIN should use node_id from schema"
        assert "(src.node_id) = ('dave')" in sql, \
            "Filter should use node_id from schema"

        # Should NOT use incorrect column name
        assert "src.id = e.src" not in sql, \
            "Should not use hardcoded 'id' column"

    def test_multiple_edge_types_with_filter(self) -> None:
        """Filter should work with multiple edge types in pattern."""
        # Add FOLLOWS edge
        edge = EdgeSchema(
            name="FOLLOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", str),
            sink_id_property=EntityProperty("dst", str),
            properties=[],
        )
        self.schema.add_edge(
            edge,
            SQLTableDescriptor(
                entity_id="Person@FOLLOWS@Person",
                table_name="test.follows",
            ),
        )

        query = """
        MATCH path = (a:Person)-[:KNOWS|FOLLOWS*1..2]->(b:Person)
        WHERE a.node_id = "eve"
        RETURN a.node_id, b.node_id
        """

        sql = self._transpile(query)

        # Filter should still be applied
        assert "(src.node_id) = ('eve')" in sql, \
            "Filter not applied with multiple edge types"

    def test_sink_filter_not_in_base_case(self) -> None:
        """Filter on sink (target) node should NOT be in base case."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person)
        WHERE b.node_id = "frank"
        RETURN a.node_id, b.node_id
        """

        sql = self._transpile(query)
        lines = sql.split('\n')

        # Find base case section
        base_case_line = next(i for i, l in enumerate(lines) if "-- Base case" in l)
        union_all_line = next(i for i, l in enumerate(lines) if "UNION ALL" in l and i > base_case_line)
        base_case_section = '\n'.join(lines[base_case_line:union_all_line])

        # Sink filter should NOT be in base case (it's applied after CTE)
        assert "b.node_id" not in base_case_section, \
            "Sink node filter should not be in base case"
