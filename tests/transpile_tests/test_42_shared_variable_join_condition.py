"""Test 42: Shared variable join condition across pattern parts.

Validates that when the same node variable appears in multiple MATCH pattern
parts, proper join conditions are generated to equate the node references.

This is a regression test for Bug #2: Missing join condition (Cartesian product).
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
    assert_has_join,
    assert_no_cartesian_join,
)


class TestSharedVariableJoinCondition:
    """Test join conditions for shared variables across pattern parts."""

    TEST_ID = "42"
    TEST_NAME = "shared_variable_join_condition"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema with Person, City, and Company
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
                    EntityProperty("country", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_node(
            NodeSchema(
                name="Company",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("industry", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("city_id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="WORKS_AT",
                source_node_id="Person",
                sink_node_id="Company",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("company_id", int),
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
                    EntityProperty("country", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.City"),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Company",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("industry", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Company"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("city_id", int),
            ),
            SQLTableDescriptor(table_name="graph.LivesIn"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="WORKS_AT",
                source_node_id="Person",
                sink_node_id="Company",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("company_id", int),
            ),
            SQLTableDescriptor(table_name="graph.WorksAt"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_shared_variable_two_patterns_basic(self) -> None:
        """Test that shared variable 'p' generates proper join condition.

        Bug #2 scenario: Same node variable in multiple pattern parts creates
        a Cartesian join with ON TRUE instead of proper join condition.

        Query:
            MATCH (p:Person)-[:LIVES_IN]->(c:City),
                  (p)-[:WORKS_AT]->(co:Company)
            RETURN p.name

        Expected: The second reference to 'p' should be constrained to equal
        the first reference, NOT create a Cartesian product.
        """
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City),
              (p)-[:WORKS_AT]->(co:Company)
        RETURN p.name
        """
        sql = self._transpile(cypher)

        # Critical check: There should NOT be "ON TRUE" which indicates Cartesian
        # Use the assertion helper which handles multi-line patterns
        assert_no_cartesian_join(sql)

        # There should be proper joins
        assert_has_join(sql)

    def test_shared_variable_with_filters(self) -> None:
        """Test the exact bug scenario from Query 25.

        Query:
            MATCH (p:Person)-[:LIVES_IN]->(c:City),
                  (p)-[:WORKS_AT]->(co:Company)
            WHERE c.country = 'USA' AND co.industry = 'Tech'
            RETURN p.name, c.name AS city, co.name AS company

        This is the exact query that triggers Bug #2.
        """
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City),
              (p)-[:WORKS_AT]->(co:Company)
        WHERE c.country = 'USA' AND co.industry = 'Tech'
        RETURN p.name, c.name AS city, co.name AS company
        """
        sql = self._transpile(cypher)

        # No Cartesian joins should exist
        assert_no_cartesian_join(sql)

        # Verify filters are present
        assert "usa" in sql.lower() or "USA" in sql
        assert "tech" in sql.lower() or "Tech" in sql

    def test_shared_variable_no_cartesian_product(self) -> None:
        """Verify no Cartesian product for shared variables."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City),
              (p)-[:WORKS_AT]->(co:Company)
        RETURN p.name
        """
        sql = self._transpile(cypher)
        assert_no_cartesian_join(sql)

    def test_separate_variables_no_false_positive(self) -> None:
        """Ensure different variable names don't get falsely joined.

        This test ensures the fix doesn't incorrectly join unrelated variables.
        """
        cypher = """
        MATCH (p1:Person)-[:LIVES_IN]->(c:City),
              (p2:Person)-[:WORKS_AT]->(co:Company)
        RETURN p1.name, p2.name
        """
        sql = self._transpile(cypher)

        # This should have two separate Person references - not joined together
        # The query should succeed without error
        assert "p1" in sql.lower() or "__p1_" in sql
        assert "p2" in sql.lower() or "__p2_" in sql

    def test_shared_variable_three_patterns(self) -> None:
        """Test shared variable across three pattern parts."""
        # Add a third edge type for this test
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("friend_id", int),
            )
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("friend_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City),
              (p)-[:WORKS_AT]->(co:Company),
              (p)-[:KNOWS]->(f:Person)
        RETURN p.name, c.name AS city, co.name AS company, f.name AS friend
        """
        sql = self._transpile(cypher)

        # No Cartesian joins
        assert_no_cartesian_join(sql)

    def test_multiple_shared_variables(self) -> None:
        """Test multiple shared variables across patterns."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City),
              (p)-[:WORKS_AT]->(co:Company),
              (c)<-[:LIVES_IN]-(p2:Person)
        WHERE p <> p2
        RETURN p.name, p2.name, c.name AS city
        """
        sql = self._transpile(cypher)

        # Both p and c are shared - no Cartesian joins
        assert_no_cartesian_join(sql)
