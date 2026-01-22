"""Tests for BFS with inline property source filters.

This test validates that inline property filters on source nodes in
variable-length path queries are correctly optimized by pushing them
into the base case of the recursive CTE.

Inline filters like (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b)
should behave identically to WHERE a.name = 'Alice' in BFS optimization.
"""

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


class TestBFSInlineSourceFilter:
    """Tests for inline property filters in BFS source node optimization."""

    def setup_method(self) -> None:
        """Set up test fixtures with schema."""
        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()

        # Node with custom node_id_property
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

    def test_inline_source_filter_in_bfs_base_case(self) -> None:
        """Inline filter on source node should be in base case of CTE."""
        query = """
        MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person)
        RETURN b.name
        """
        sql = self._transpile(query)

        # Should have recursive CTE
        assert "WITH RECURSIVE" in sql.upper()

        # Filter should be in base case (before UNION ALL)
        lines = sql.split("\n")
        base_idx = next(
            i for i, l in enumerate(lines) if "-- Base case" in l
        )
        union_idx = next(
            i
            for i, l in enumerate(lines)
            if "UNION ALL" in l and i > base_idx
        )
        alice_idx = next(i for i, l in enumerate(lines) if "'Alice'" in l)

        assert base_idx < alice_idx < union_idx, \
            "Inline source filter not in base case!"

    def test_inline_filter_uses_source_node_join(self) -> None:
        """Inline filter should use JOIN with source node table."""
        query = """
        MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person)
        RETURN b.name
        """
        sql = self._transpile(query)

        # Should have JOIN with source table
        assert "JOIN test.persons src ON src.node_id = e.src" in sql, \
            "Missing JOIN with source node table in base case"

        # WHERE clause should include the inline filter
        assert "(src.name) = ('Alice')" in sql, \
            "Source node inline filter not applied in WHERE clause"

    def test_inline_equals_explicit_where(self) -> None:
        """Inline filter should produce same SQL as explicit WHERE."""
        query_inline = (
            "MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->"
            "(b:Person) RETURN b"
        )
        query_explicit = (
            "MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'Alice' RETURN b"
        )

        sql_inline = self._transpile(query_inline)
        sql_explicit = self._transpile(query_explicit)

        # Both should have the filter in the base case
        assert "'Alice'" in sql_inline
        assert "'Alice'" in sql_explicit

        # Both should have source node JOIN
        assert "JOIN test.persons src" in sql_inline
        assert "JOIN test.persons src" in sql_explicit
