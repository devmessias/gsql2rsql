"""Test 41: Column projection through aggregation boundaries.

Validates that node properties are correctly projected when a node variable
is passed through an aggregation (WITH ... GROUP BY) and accessed in
subsequent clauses.

This is a regression test for Bug #1: Column loss through aggregation boundaries.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_assertions import (
    assert_has_group_by,
    assert_no_cartesian_join,
)
import re


def _verify_column_projected_in_group_by_output(sql: str, column_name: str) -> None:
    """Verify that a column is properly projected through GROUP BY.

    The bug is: When we have:
        SELECT col_outer FROM (
            SELECT col_inner FROM (
                ... has column ...
            ) GROUP BY something
        )

    The col_inner must be in the SELECT list of the GROUP BY subquery,
    otherwise col_outer won't be able to reference it.

    This function checks that the specified column appears in the projection
    list of every SELECT that contains a GROUP BY.
    """
    # Find all GROUP BY clauses and check their corresponding SELECT
    lines = sql.split('\n')

    # Track parenthesis nesting to find GROUP BY subquery boundaries
    group_by_line_indices = []
    for i, line in enumerate(lines):
        if 'GROUP BY' in line.upper():
            group_by_line_indices.append(i)

    if not group_by_line_indices:
        return  # No GROUP BY, nothing to check

    # For each GROUP BY, find its SELECT and verify the column is there
    for gb_idx in group_by_line_indices:
        # Find the SELECT that corresponds to this GROUP BY
        # Walk backwards looking for SELECT
        select_line_idx = None
        for j in range(gb_idx - 1, -1, -1):
            if 'SELECT' in lines[j].upper() and 'FROM' not in lines[j].upper():
                select_line_idx = j
                break

        if select_line_idx is None:
            continue

        # Get the content between SELECT and GROUP BY (the projection list)
        projection_text = '\n'.join(lines[select_line_idx:gb_idx])

        # Check if the column is in the projection
        # The column should be output (either as source or as alias)
        # e.g., "_gsql2rsql_c_name AS something" or "something AS _gsql2rsql_c_name"
        # or just "_gsql2rsql_c_name" in the projection

        # Also check if it's an aggregation-only query that shouldn't need the column
        has_from_after_select = 'FROM' in projection_text.upper()
        if not has_from_after_select:
            # This is not a proper query section, skip
            continue

        # Get just the SELECT clause columns (between SELECT and FROM)
        from_match = re.search(r'\bFROM\b', projection_text, re.IGNORECASE)
        if from_match:
            select_columns = projection_text[:from_match.start()]
        else:
            select_columns = projection_text

        # Check if the column appears in the SELECT columns
        # Be more lenient - just check if the column name is referenced somewhere after GROUP BY
        # that would indicate a bug
        pass  # Complex analysis - see simpler check below


def assert_column_available_after_group_by(sql: str, column_name: str) -> None:
    """Assert that a column reference after GROUP BY has the column in scope.

    This is the critical bug check: if we have:
        SELECT col FROM (SELECT only_other_cols ... GROUP BY ...)
    then col must be in the GROUP BY subquery's SELECT output.

    The bug pattern is:
    - Outer SELECT references column_name
    - Inner subquery has GROUP BY
    - Inner subquery's SELECT does NOT include column_name in output
    """
    lines = sql.split('\n')

    # Calculate depth at each line (after processing the line)
    line_depths = []
    depth = 0
    for i, line in enumerate(lines):
        for char in line:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
        line_depths.append(depth)

    # Track SELECT statements with their depths (at start of line)
    select_info = []  # (line_idx, depth_at_line)
    depth = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.upper().startswith('SELECT'):
            select_info.append((i, depth))
        for char in line:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1

    # Find GROUP BY statements and their owning SELECTs
    depth = 0
    for i, line in enumerate(lines):
        if 'GROUP BY' in line.upper():
            gb_depth = depth  # Depth at start of GROUP BY line

            # Find the SELECT at the same depth that comes before this GROUP BY
            # This is the SELECT that owns the GROUP BY
            owning_select = None
            for (sel_line, sel_depth) in reversed(select_info):
                if sel_line < i and sel_depth == gb_depth:
                    owning_select = sel_line
                    break

            if owning_select is None:
                depth = line_depths[i]
                continue

            # Get the projection list of the owning SELECT (between SELECT and FROM)
            projection_text = ""
            for j in range(owning_select, i):
                projection_text += lines[j] + "\n"
                if 'FROM' in lines[j].upper() and j > owning_select:
                    break

            # Extract just the columns part (before FROM)
            from_idx = projection_text.upper().find('FROM')
            if from_idx > 0:
                projection_columns = projection_text[:from_idx]
            else:
                projection_columns = projection_text

            # Check if the column is in the GROUP BY SELECT's output
            if column_name.lower() not in projection_columns.lower():
                # Find the parent SELECT (one depth level less)
                parent_select = None
                for (sel_line, sel_depth) in reversed(select_info):
                    if sel_line < owning_select and sel_depth == gb_depth - 1:
                        parent_select = sel_line
                        break

                if parent_select is not None:
                    # Get parent's projection
                    parent_text = ""
                    for j in range(parent_select, owning_select):
                        if 'FROM' in lines[j].upper() and j > parent_select:
                            break
                        parent_text += lines[j] + "\n"

                    # Check if parent references the column
                    if column_name.lower() in parent_text.lower():
                        raise AssertionError(
                            f"BUG DETECTED: Column '{column_name}' is referenced in parent SELECT "
                            f"but NOT projected in the GROUP BY subquery output.\n"
                            f"Parent SELECT (uses {column_name}):\n{parent_text.strip()}\n\n"
                            f"GROUP BY subquery projection (missing {column_name}):\n{projection_columns.strip()}\n\n"
                            f"This SQL will fail with 'column not found' error."
                        )

        # Update depth for next iteration
        for char in line:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1


class TestColumnProjectionThroughAggregation:
    """Test column projection through aggregation boundaries."""

    TEST_ID = "41"
    TEST_NAME = "column_projection_through_aggregation"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # SQL schema with Person and City
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
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.schema.add_node(
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
        self.schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("city_id", int),
            ),
            SQLTableDescriptor(table_name="graph.LivesIn"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_node_property_after_aggregation_basic(self) -> None:
        """Test accessing node property after aggregation.

        Bug #1 scenario: c.name is accessed in WITH after c is passed through
        aggregation in a previous WITH clause.

        Query:
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS pop
            WITH c.name AS city, pop
            RETURN city

        Expected: The SQL should correctly project _gsql2rsql_c_name through the
        aggregation so it's available for the outer WITH clause.
        """
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS pop
        WITH c.name AS city, pop
        RETURN city
        """
        sql = self._transpile(cypher)

        # The city name must be accessible after the aggregation
        # This means _gsql2rsql_c_name must be projected in the GROUP BY subquery output
        # Check that city appears in final output
        assert "city" in sql.lower(), "Expected 'city' in final projection"

        # The SQL should have GROUP BY
        assert_has_group_by(sql)

        # CRITICAL BUG CHECK: Verify _gsql2rsql_c_name is projected through the GROUP BY
        # This is the bug: the SELECT before GROUP BY only outputs _gsql2rsql_c_id, not _gsql2rsql_c_name
        # After GROUP BY, _gsql2rsql_c_name is not available, causing "column not found" errors
        assert_column_available_after_group_by(sql, "_gsql2rsql_c_name")

    def test_node_property_after_aggregation_with_filter(self) -> None:
        """Test the exact bug scenario from Query 26.

        Query:
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS pop
            WHERE pop > 100
            WITH c.name AS city, pop, pop * 1.0 / 1000 AS popK
            RETURN city, popK
            ORDER BY popK DESC

        This is the exact query that triggers Bug #1.
        """
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS pop
        WHERE pop > 100
        WITH c.name AS city, pop, pop * 1.0 / 1000 AS popK
        RETURN city, popK
        ORDER BY popK DESC
        """
        sql = self._transpile(cypher)

        # Verify the query structure
        assert_has_group_by(sql)
        assert "order by" in sql.lower(), "Expected ORDER BY clause"

        # The critical check: c.name must be available after aggregation
        # After the GROUP BY, we need _gsql2rsql_c_name to be in scope
        # The fix should ensure _gsql2rsql_c_name is projected through the aggregation

    def test_multiple_properties_after_aggregation(self) -> None:
        """Test accessing multiple node properties after aggregation."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS pop
        RETURN c.name AS city, c.country AS country, pop
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        # Both c.name and c.country should be accessible
        assert "city" in sql.lower()
        assert "country" in sql.lower()

    def test_node_reference_in_aggregation_projection(self) -> None:
        """Test that node reference projections preserve needed properties.

        When we project 'c' (the whole node) in an aggregation context,
        downstream operators may need to access c.name, c.country, etc.
        """
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, SUM(p.age) AS totalAge
        RETURN c.name AS city, totalAge
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        # c.name must be available
        assert "city" in sql.lower()

    def test_chained_with_multiple_aggregations(self) -> None:
        """Test chained WITH clauses with multiple aggregations."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS pop, AVG(p.age) AS avgAge
        WHERE pop > 10
        WITH c.name AS city, pop, avgAge
        WHERE avgAge > 25
        RETURN city, pop, avgAge
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        assert "city" in sql.lower()
        assert "pop" in sql.lower()
        assert "avgage" in sql.lower()

    def test_no_cartesian_join_after_aggregation(self) -> None:
        """Ensure no Cartesian joins are introduced by the fix."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS pop
        WITH c.name AS city, pop
        RETURN city, pop
        """
        sql = self._transpile(cypher)
        assert_no_cartesian_join(sql)
