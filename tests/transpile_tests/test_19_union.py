"""Test 19: UNION set operation.

Validates that UNION and UNION ALL are correctly transpiled to SQL.

IMPORTANT: This is a TDD test. SetOperator already exists, but we need
to verify it generates correct SQL with proper handling of ORDER BY/LIMIT.
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql


class TestUnion:
    """Test UNION set operation transpilation."""

    TEST_ID = "19"
    TEST_NAME = "union"

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
                name="City",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
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
                name="City",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.City"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name
        UNION
        MATCH (c:City)
        RETURN c.name AS name
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-19"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_union_keyword(self) -> None:
        """Test SQL has UNION keyword."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name
        UNION
        MATCH (c:City)
        RETURN c.name AS name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "UNION" in sql_upper, "Should have UNION keyword"
        # Should NOT have UNION ALL (UNION removes duplicates)
        assert "UNION ALL" not in sql_upper, "UNION should not be UNION ALL"

    def test_union_all(self) -> None:
        """Test UNION ALL (keeps duplicates)."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name
        UNION ALL
        MATCH (c:City)
        RETURN c.name AS name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "UNION ALL" in sql_upper, "Should have UNION ALL keyword"

    def test_union_references_both_tables(self) -> None:
        """Test UNION references both source tables."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name
        UNION
        MATCH (c:City)
        RETURN c.name AS name
        """
        sql = self._transpile(cypher)

        sql_lower = sql.lower()
        assert "person" in sql_lower, "Should reference Person table"
        assert "city" in sql_lower, "Should reference City table"

    def test_union_with_type_column(self) -> None:
        """Test UNION with additional type discriminator column."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name, 'Person' AS type
        UNION
        MATCH (c:City)
        RETURN c.name AS name, 'City' AS type
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "UNION" in sql_upper
        # Should have both type literals
        assert "'Person'" in sql or "'PERSON'" in sql.upper()
        assert "'City'" in sql or "'CITY'" in sql.upper()

    def test_union_no_cartesian_join(self) -> None:
        """Test UNION does NOT produce cartesian join."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name
        UNION
        MATCH (c:City)
        RETURN c.name AS name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should NOT have ON TRUE (cartesian join)
        assert "ON TRUE" not in sql_upper, "Should not have cartesian join"
        # Should NOT have CROSS JOIN
        assert "CROSS JOIN" not in sql_upper, "Should not have CROSS JOIN"

    def test_multiple_unions(self) -> None:
        """Test chaining multiple UNIONs."""
        # Add Movie node for this test
        self.graph_schema.add_node(
            NodeSchema(
                name="Movie",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("title", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Movie",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("title", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Movie"),
        )

        cypher = """
        MATCH (p:Person)
        RETURN p.name AS name
        UNION
        MATCH (c:City)
        RETURN c.name AS name
        UNION
        MATCH (m:Movie)
        RETURN m.title AS name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have multiple UNIONs
        union_count = sql_upper.count("UNION")
        assert union_count >= 2, f"Should have at least 2 UNION, found {union_count}"
