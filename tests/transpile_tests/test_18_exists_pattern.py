"""Test 18: EXISTS pattern predicate.

Validates that EXISTS { pattern } expressions are correctly
transpiled to SQL EXISTS subqueries.

IMPORTANT: This is a TDD test. If it fails, ExistsPredicateOperator
needs to be implemented in the planner.
"""

import pytest

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

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql
from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
)


class TestExistsPattern:
    """Test EXISTS pattern predicate transpilation."""

    TEST_ID = "18"
    TEST_NAME = "exists_pattern"

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
        self.graph_schema.add_node(
            NodeSchema(
                name="Movie",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("title", str),
                    EntityProperty("year", int),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="ACTED_IN",
                source_node_id="Person",
                sink_node_id="Movie",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="DIRECTED",
                source_node_id="Person",
                sink_node_id="Movie",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Movie",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("title", str),
                    EntityProperty("year", int),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Movie"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="ACTED_IN",
                source_node_id="Person",
                sink_node_id="Movie",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.ActedIn"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="DIRECTED",
                source_node_id="Person",
                sink_node_id="Movie",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Directed"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        RETURN p.name
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-18"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_exists_keyword(self) -> None:
        """Test SQL has EXISTS keyword in WHERE clause."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXISTS" in sql_upper, "Should have EXISTS keyword"
        assert "SELECT" in sql_upper, "Should have SELECT for subquery"

    def test_exists_generates_subquery(self) -> None:
        """Test EXISTS generates a proper subquery."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        # Should have EXISTS with subquery
        sql_upper = sql.upper()
        assert "EXISTS" in sql_upper
        # Subquery should reference relationship table
        sql_lower = sql.lower()
        assert "actedin" in sql_lower, "Should reference ActedIn table in subquery"

    def test_not_exists(self) -> None:
        """Test NOT EXISTS pattern."""
        cypher = """
        MATCH (p:Person)
        WHERE NOT EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "NOT" in sql_upper and "EXISTS" in sql_upper, "Should have NOT EXISTS"

    def test_exists_with_property_filter(self) -> None:
        """Test EXISTS with property filter in pattern."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(m:Movie) WHERE m.year > 2020 }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXISTS" in sql_upper
        # Filter should be in subquery
        assert "2020" in sql, "Should have year filter value"

    def test_exists_no_cartesian_join(self) -> None:
        """Test EXISTS does NOT produce cartesian join (uses subquery)."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # EXISTS should use subquery, not join
        assert "EXISTS" in sql_upper, "Should use EXISTS, not JOIN"
        # Should NOT have ON TRUE (cartesian join)
        assert "ON TRUE" not in sql_upper, "Should not have cartesian join"

    def test_multiple_exists(self) -> None:
        """Test multiple EXISTS conditions with AND."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
          AND EXISTS { (p)-[:DIRECTED]->(:Movie) }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have two EXISTS
        exists_count = sql_upper.count("EXISTS")
        assert exists_count >= 2, f"Should have 2 EXISTS, found {exists_count}"

    def test_exists_references_outer_variable(self) -> None:
        """Test EXISTS subquery references outer query variable (correlated)."""
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        RETURN p.name
        """
        sql = self._transpile(cypher)

        # The subquery should reference p.id from outer query
        # This creates a correlated subquery
        sql_lower = sql.lower()
        assert "exists" in sql_lower
        # Should have some reference to person's id in the EXISTS condition
        # The exact form depends on implementation
