"""Test 08: Undirected relationship match.

Validates that undirected relationship patterns -[:TYPE]- are transpiled correctly.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_from_table,
    assert_has_join,
    SQLStructure,
)


class TestUndirectedRelationship:
    """Test undirected relationship matching."""

    TEST_ID = "08"
    TEST_NAME = "undirected_relationship"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_undirected_parses_successfully(self) -> None:
        """Test that undirected pattern parses without error."""
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        # Should not raise an exception
        sql = self._transpile(cypher)
        assert_has_select(sql)

    def test_undirected_generates_joins(self) -> None:
        """Test that undirected pattern generates JOINs."""
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        assert_has_join(sql)

    def test_undirected_references_both_node_tables(self) -> None:
        """Test that undirected references Person table for both nodes."""
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Person")

    def test_undirected_references_edge_table(self) -> None:
        """Test that undirected references edge table."""
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Knows")

    def test_forward_direction_works(self) -> None:
        """Test that forward direction -[:TYPE]-> works."""
        cypher = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        assert_has_select(sql)
        assert_has_join(sql)

    def test_backward_direction_works(self) -> None:
        """Test that backward direction <-[:TYPE]- works."""
        cypher = "MATCH (p:Person)<-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        assert_has_select(sql)
        assert_has_join(sql)

    def test_undirected_with_where_filter(self) -> None:
        """Test undirected pattern with WHERE filter."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice'
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_select(sql)
        structure = SQLStructure(raw_sql=sql)
        assert structure.has_where

    def test_undirected_produces_valid_sql(self) -> None:
        """Test that undirected pattern produces valid SQL structure."""
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        # Should have valid SQL components
        assert "SELECT" in sql.upper()
        assert "FROM" in sql.upper()
        # Should project both names
        assert "name" in sql.lower()

    def test_undirected_queries_both_directions(self) -> None:
        """Test that undirected pattern queries both forward and reverse directions.

        An undirected relationship -[:KNOWS]- should match edges where:
        - p is the source AND f is the target, OR
        - f is the source AND p is the target

        The SQL should either:
        - Use OR condition in join: (p.id = source_id AND f.id = target_id) OR (p.id = target_id AND f.id = source_id)
        - Or use UNION of both directions
        """
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"
        sql = self._transpile(cypher)

        sql_lower = sql.lower()

        # The SQL must query both directions
        # Check for UNION approach
        has_union = "union" in sql_lower

        # Check for OR approach in join conditions
        # The join conditions should have OR with both source_id and target_id (possibly aliased)
        # Look for patterns like: "(x = y_source_id OR x = y_target_id)"
        has_or_in_joins = " or " in sql_lower

        # Also verify both source and target columns appear in the SQL
        has_source_col = "source_id" in sql_lower
        has_target_col = "target_id" in sql_lower

        # For proper undirected semantics with OR approach:
        # - Must have OR in join condition
        # - Must reference both source and target columns
        has_both_direction_joins = has_or_in_joins and has_source_col and has_target_col

        assert has_union or has_both_direction_joins, (
            f"Undirected relationship should query both directions. "
            f"Expected either UNION or OR conditions referencing both source_id and target_id. "
            f"has_or={has_or_in_joins}, has_source={has_source_col}, has_target={has_target_col}. "
            f"SQL:\n{sql}"
        )
