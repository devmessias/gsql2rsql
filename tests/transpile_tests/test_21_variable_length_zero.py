"""Test 21: Variable-Length Path with Zero-Length (*0..N).

Validates that variable-length paths with min_hops=0 correctly include
zero-length paths (identity paths) in the results.

Zero-length paths mean the starting node itself is included, representing
an "identity" relationship where no edges are traversed.
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


class TestVariableLengthZero:
    """Test variable-length paths with zero-length support (*0..N)."""

    TEST_ID = "21"
    TEST_NAME = "variable_length_zero"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema with Person nodes and KNOWS relationships
        self.graph_schema = SimpleGraphSchemaProvider()

        person_node = NodeSchema(
            name="Person",
            properties=[
                EntityProperty("id", int),
                EntityProperty("name", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("id", int),
        )
        self.graph_schema.add_node(person_node)

        knows_edge = EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("source_id", int),
            sink_id_property=EntityProperty("target_id", int),
        )
        self.graph_schema.add_edge(knows_edge)

        # For multi-edge type tests
        follows_edge = EdgeSchema(
            name="FOLLOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("source_id", int),
            sink_id_property=EntityProperty("target_id", int),
        )
        self.graph_schema.add_edge(follows_edge)

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
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
        self.sql_schema.add_edge(
            EdgeSchema(
                name="FOLLOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Follows"),
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
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        RETURN DISTINCT f.name
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-21"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_recursive_cte(self) -> None:
        """Test SQL has WITH RECURSIVE CTE."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "WITH RECURSIVE" in sql_upper, "Should have WITH RECURSIVE CTE"
        assert "UNION ALL" in sql_upper, "CTE should have UNION ALL"

    def test_includes_zero_depth_base_case(self) -> None:
        """Test CTE includes depth=0 base case for zero-length paths."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have a base case with depth = 0
        assert "0 AS DEPTH" in sql_upper or "0 AS `DEPTH`" in sql_upper, \
            "Should have depth=0 base case for zero-length paths"

        # Should have WHERE depth >= 0 (not >= 1)
        assert "DEPTH >= 0" in sql_upper or "`DEPTH` >= 0" in sql_upper, \
            "Should allow depth >= 0 to include zero-length paths"

    def test_includes_starting_node(self) -> None:
        """Test that zero-length path includes starting node in results.

        For *0..2, the depth=0 case means start_node = end_node (identity).
        This should be reflected in the CTE base case.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_lower = sql.lower()
        # Depth=0 base case should have same column as start and end
        # Look for pattern like: "id AS start_node, id AS end_node" or similar
        # This is implementation-dependent, but we can check for depth=0
        assert "0 as depth" in sql_lower or "0 as `depth`" in sql_lower, \
            "Should have depth=0 rows representing identity paths"

    def test_zero_to_three_hops(self) -> None:
        """Test *0..3 range works correctly."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..3]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have WITH RECURSIVE
        assert "WITH RECURSIVE" in sql_upper, "Should use recursive CTE"

        # Should have depth >= 0 and depth <= 3
        assert "DEPTH >= 0" in sql_upper or "`DEPTH` >= 0" in sql_upper, \
            "Should include depth=0"
        assert "DEPTH <= 3" in sql_upper or "`DEPTH` <= 3" in sql_upper, \
            "Should limit to max depth 3"

        # Should have depth < 3 in recursive case (stops before going to depth 4)
        assert "DEPTH < 3" in sql_upper or "`DEPTH` < 3" in sql_upper, \
            "Recursive case should stop before exceeding max depth"

    def test_zero_to_unbounded(self) -> None:
        """Test *0.. syntax (unbounded from zero)."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have WITH RECURSIVE
        assert "WITH RECURSIVE" in sql_upper, "Should use recursive CTE"

        # Should have depth >= 0 (no upper bound in WHERE)
        assert "DEPTH >= 0" in sql_upper or "`DEPTH` >= 0" in sql_upper, \
            "Should include depth=0"

        # Should NOT have depth <= N in final WHERE (unbounded)
        # But should have cycle detection
        assert "ARRAY_CONTAINS" in sql_upper or "ARRAY_CONTAINS" in sql, \
            "Should have cycle detection for unbounded traversal"

    def test_multiple_edge_types_with_zero(self) -> None:
        """Test [:KNOWS|FOLLOWS*0..2] with multiple edge types."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*0..2]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have WITH RECURSIVE
        assert "WITH RECURSIVE" in sql_upper, "Should use recursive CTE"

        # Should have depth=0 base case (same for all edge types)
        assert "0 AS DEPTH" in sql_upper or "0 AS `DEPTH`" in sql_upper, \
            "Should have depth=0 base case"

        # Should reference both edge tables
        assert "KNOWS" in sql_upper, "Should reference KNOWS edge table"
        assert "FOLLOWS" in sql_upper, "Should reference FOLLOWS edge table"

        # Should have depth >= 0
        assert "DEPTH >= 0" in sql_upper or "`DEPTH` >= 0" in sql_upper, \
            "Should include zero-length paths"

    def test_no_cartesian_join(self) -> None:
        """Test that *0..N does NOT produce cartesian joins."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should NOT have ON TRUE (cartesian join indicator)
        assert "ON TRUE" not in sql_upper, "Should not have cartesian join"
        assert "ON (TRUE)" not in sql_upper, "Should not have cartesian join"

        # Should NOT have CROSS JOIN
        assert "CROSS JOIN" not in sql_upper, "Should not have CROSS JOIN"

    def test_pure_zero_length(self) -> None:
        """Test *0..0 (only zero-length paths, no traversal)."""
        cypher = """
        MATCH (p:Person {name: 'Alice'})-[:KNOWS*0..0]->(f:Person)
        RETURN f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # For *0..0, should only have depth=0 (no recursive case needed)
        # Should have depth >= 0 AND depth <= 0
        assert "DEPTH >= 0" in sql_upper or "`DEPTH` >= 0" in sql_upper, \
            "Should include depth=0"
        assert "DEPTH <= 0" in sql_upper or "`DEPTH` <= 0" in sql_upper, \
            "Should limit to depth=0 only"

        # Should still have WITH RECURSIVE structure (even if no recursion happens)
        assert "WITH RECURSIVE" in sql_upper, "Should use recursive CTE structure"

    def test_with_where_on_target(self) -> None:
        """Test *0..N with WHERE clause on target node."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        WHERE f.age > 30
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have WITH RECURSIVE
        assert "WITH RECURSIVE" in sql_upper, "Should use recursive CTE"

        # Should have depth=0 base case
        assert "0 AS DEPTH" in sql_upper or "0 AS `DEPTH`" in sql_upper, \
            "Should have depth=0 base case"

        # Should have WHERE filter on age
        # The age column should be projected and filtered
        assert "__F_AGE" in sql_upper or "AGE" in sql_upper, \
            "Should reference age column"
        assert "> 30" in sql or ">30" in sql or "> (30)" in sql, \
            "Should filter age > 30"
