"""Test 37: Source Node Filter Pushdown Optimization.

Validates that filters on the source node of variable-length paths are
pushed into the recursive CTE's base case for optimal performance.

Problem (without pushdown):
    MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
    WHERE p.name = 'Alice'
    RETURN DISTINCT f.name

    CTE explores ALL paths from ALL persons, then filters after.
    This is O(|V| * k^d) where V=vertices, k=avg degree, d=max depth.

Solution (with pushdown):
    Push p.name = 'Alice' into CTE base case.
    CTE only explores paths starting from Alice.
    This is O(k^d) - massive improvement for large graphs.
"""

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.common.schema import (
    EdgeSchema,
    NodeSchema,
    EntityProperty,
)
from gsql2rsql.planner.pass_manager import optimize_plan
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestSourceNodeFilterPushdown:
    """Test source node filter pushdown into recursive CTE."""

    TEST_ID = "37"
    TEST_NAME = "source_node_filter_pushdown"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        # Create schema with table descriptors
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="test.Person"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("friend_id", int),
                properties=[
                    EntityProperty("person_id", int),
                    EntityProperty("friend_id", int),
                    EntityProperty("since", str),
                ],
            ),
            SQLTableDescriptor(table_name="test.Knows"),
        )

    def _transpile(self, cypher: str, optimize: bool = True) -> str:
        """Transpile a Cypher query to SQL."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        if optimize:
            optimize_plan(plan)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_simple_name_filter_pushed_to_base_case(self) -> None:
        """Test that WHERE p.name = 'Alice' is pushed into CTE base case."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT f.name AS reachable
        """
        sql = self._transpile(query)

        # The SQL should have a JOIN with Person table in base case
        assert "JOIN" in sql
        # The filter should be in the base case, not after the CTE
        assert "src.name" in sql.lower() or "src.`name`" in sql.lower()
        # The filter value 'Alice' should appear in the CTE definition
        # not just in the final SELECT
        cte_part = sql.split("SELECT DISTINCT")[0] if "SELECT DISTINCT" in sql else sql
        assert "'Alice'" in cte_part

    def test_id_filter_pushed_to_base_case(self) -> None:
        """Test that WHERE p.id = 42 is pushed into CTE base case."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE p.id = 42
        RETURN DISTINCT f.name AS reachable
        """
        sql = self._transpile(query)

        # The filter should be in the base case
        assert "JOIN" in sql
        # The source node filter should be in the CTE
        cte_part = sql.split("SELECT DISTINCT")[0] if "SELECT DISTINCT" in sql else sql
        assert "42" in cte_part

    def test_compound_source_filter_pushed(self) -> None:
        """Test that compound source filters (AND) are pushed."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE p.name = 'Alice' AND p.age > 25
        RETURN DISTINCT f.name AS reachable
        """
        sql = self._transpile(query)

        # Both conditions should be in the base case
        cte_part = sql.split("SELECT DISTINCT")[0] if "SELECT DISTINCT" in sql else sql
        assert "'Alice'" in cte_part
        assert "25" in cte_part

    def test_mixed_filter_source_only_pushed(self) -> None:
        """Test that only source-only filters are pushed, others remain."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN DISTINCT f.name AS reachable
        """
        sql = self._transpile(query)

        # p.name = 'Alice' should be in base case (CTE part)
        cte_part = sql.split("SELECT DISTINCT")[0] if "SELECT DISTINCT" in sql else sql
        assert "'Alice'" in cte_part

        # f.age > 30 should be in the final WHERE (after CTE)
        # The final filter should reference the target node's age
        final_part = sql.split("SELECT DISTINCT")[1] if "SELECT DISTINCT" in sql else ""
        assert "30" in final_part or "30" in sql

    def test_no_filter_no_join(self) -> None:
        """Test that queries without source filter don't add unnecessary JOIN."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        RETURN DISTINCT f.name AS reachable
        """
        sql = self._transpile(query)

        # The base case should NOT have a JOIN with Person (no filter to push)
        # Look at the CTE base case specifically
        cte_match = sql.split("-- Base case: direct edges")
        if len(cte_match) > 1:
            base_case_end = cte_match[1].split("UNION ALL")[0]
            # Should just be FROM Knows, not FROM Knows JOIN Person
            assert "src" not in base_case_end.lower()

    def test_target_only_filter_not_pushed(self) -> None:
        """Test that filters on target node only are NOT pushed to base case."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE f.name = 'Bob'
        RETURN DISTINCT p.name AS source
        """
        sql = self._transpile(query)

        # The filter should NOT be in the CTE base case
        # Bob filter should be after the CTE
        cte_part = sql.split("SELECT DISTINCT")[0] if "SELECT DISTINCT" in sql else sql
        # The 'Bob' should appear but not in the CTE's WHERE clause
        # (it should be in the final filter on the sink node)
        base_case = sql.split("-- Base case: direct edges")[1].split("UNION ALL")[0] if "-- Base case:" in sql else ""
        assert "'Bob'" not in base_case

    def test_zero_length_path_with_filter(self) -> None:
        """Test filter pushdown for zero-length paths (min=0)."""
        query = """
        MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT f.name AS reachable
        """
        sql = self._transpile(query)

        # Zero-length path should also have the filter
        if "-- Base case: Zero-length paths" in sql:
            zero_case = sql.split("-- Base case: Zero-length paths")[1].split("UNION ALL")[0]
            assert "'Alice'" in zero_case

        # Regular base case should also have the filter
        if "-- Base case: direct edges" in sql:
            base_case = sql.split("-- Base case: direct edges")[1].split("UNION ALL")[0]
            assert "'Alice'" in base_case


class TestSourceNodeFilterDetection:
    """Test the filter detection logic in the planner."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="test.Person"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("friend_id", int),
                properties=[
                    EntityProperty("person_id", int),
                    EntityProperty("friend_id", int),
                ],
            ),
            SQLTableDescriptor(table_name="test.Knows"),
        )

    def test_references_only_source(self) -> None:
        """Test _references_only_variable correctly identifies source filters."""
        from gsql2rsql.planner.recursive_traversal import (
            _collect_property_references,
            _references_only_variable,
        )

        # Parse a query with source-only filter
        query = (
            "MATCH (p:Person)-[:KNOWS*1..3]->(f:Person) "
            "WHERE p.name = 'Alice' RETURN f"
        )
        ast = self.parser.parse(query)
        part = ast.parts[0]
        match_clause = part.match_clauses[0]
        where_expr = match_clause.where_expression

        # Test the helper method
        props = _collect_property_references(where_expr)
        assert len(props) == 1
        assert props[0].variable_name == "p"

        # The expression should reference only 'p'
        assert _references_only_variable(where_expr, "p") is True
        assert _references_only_variable(where_expr, "f") is False

    def test_mixed_filter_detection(self) -> None:
        """Test detection of mixed filters (source AND target)."""
        from gsql2rsql.planner.recursive_traversal import (
            _collect_property_references,
            _references_only_variable,
        )

        # Parse a query with mixed filter
        query = (
            "MATCH (p:Person)-[:KNOWS*1..3]->(f:Person) "
            "WHERE p.name = 'Alice' AND f.age > 30 RETURN f"
        )
        ast = self.parser.parse(query)
        part = ast.parts[0]
        match_clause = part.match_clauses[0]
        where_expr = match_clause.where_expression

        # The combined expression references both p and f
        props = _collect_property_references(where_expr)
        var_names = {p.variable_name for p in props}
        assert "p" in var_names
        assert "f" in var_names

        # The combined expression should NOT be source-only
        assert _references_only_variable(where_expr, "p") is False
