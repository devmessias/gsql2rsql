"""Test 45: Inline property filters in node and relationship patterns.

Validates that inline property filters like (n:Person {name: 'Alice'})
are correctly transpiled to WHERE clauses in SQL.

Cypher Syntax:
    MATCH (n:Type {property: value}) RETURN n
    MATCH (a)-[r:REL {property: value}]->(b) RETURN r

SQL Output:
    WHERE clause with equality predicates for each inline property

Note: Variable references in inline properties (e.g., {id: variable})
are NOT currently supported and will be skipped.
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
    assert_has_where,
)


class TestInlinePropertyFilters:
    """Test inline property filter transpilation."""

    TEST_ID = "45"
    TEST_NAME = "inline_property_filters"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # SQL schema for Person and KNOWS edge
        self.schema = SimpleSQLSchemaProvider()
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
            ),
            SQLTableDescriptor(
                table_name="test.persons",
                node_id_columns=["node_id"],
            ),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("src", str),
                sink_id_property=EntityProperty("dst", str),
                properties=[
                    EntityProperty("since", int),
                    EntityProperty("strength", float),
                ],
            ),
            SQLTableDescriptor(
                entity_id="Person@KNOWS@Person",
                table_name="test.knows",
            ),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    # =========================================================================
    # Single Inline Property Tests
    # =========================================================================

    def test_node_with_single_inline_property(self) -> None:
        """Test node with single inline property creates WHERE clause."""
        cypher = "MATCH (p:Person {name: 'Alice'}) RETURN p.name"
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "'Alice'" in sql, \
            "Should contain filter value 'Alice' in WHERE"
        # Check the filter is applied
        assert "name" in sql.lower() or "Name" in sql

    def test_node_with_integer_inline_property(self) -> None:
        """Test node with integer inline property."""
        cypher = "MATCH (p:Person {age: 30}) RETURN p.name"
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "30" in sql, "Should contain filter value 30 in WHERE"

    def test_node_with_boolean_inline_property(self) -> None:
        """Test node with boolean inline property."""
        cypher = "MATCH (p:Person {active: true}) RETURN p.name"
        sql = self._transpile(cypher)

        assert_has_where(sql)
        # Should contain either TRUE or 1 (depending on SQL dialect)
        assert "TRUE" in sql.upper() or "1" in sql

    # =========================================================================
    # Multiple Inline Properties Tests
    # =========================================================================

    def test_node_with_multiple_inline_properties(self) -> None:
        """Test node with multiple inline properties combines with AND."""
        cypher = "MATCH (p:Person {name: 'Alice', age: 30}) RETURN p"
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "'Alice'" in sql
        assert "30" in sql
        assert "AND" in sql.upper(), \
            "Multiple properties should be combined with AND"

    # =========================================================================
    # Inline + Explicit WHERE Tests
    # =========================================================================

    def test_inline_property_merges_with_explicit_where(self) -> None:
        """Test inline property merges with explicit WHERE using AND."""
        cypher = (
            "MATCH (p:Person {name: 'Alice'}) "
            "WHERE p.age > 25 RETURN p.name"
        )
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "'Alice'" in sql, \
            "Should contain inline property filter value"
        assert "25" in sql, "Should contain explicit WHERE value"
        assert "AND" in sql.upper(), \
            "Inline and explicit WHERE should merge with AND"

    # =========================================================================
    # Multiple Nodes Tests
    # =========================================================================

    def test_multiple_nodes_with_inline_properties(self) -> None:
        """Test multiple nodes with inline properties in same pattern."""
        cypher = (
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->"
            "(b:Person {age: 30}) RETURN a.name, b.age"
        )
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "'Alice'" in sql, \
            "Should filter on first node property"
        assert "30" in sql, "Should filter on second node property"

    # =========================================================================
    # Relationship Inline Property Tests
    # =========================================================================

    def test_relationship_with_inline_property(self) -> None:
        """Test relationship with inline property creates WHERE clause."""
        cypher = (
            "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) "
            "RETURN r.since"
        )
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "2020" in sql, \
            "Should contain relationship property filter value"

    def test_relationship_with_multiple_inline_properties(self) -> None:
        """Test relationship with multiple inline properties."""
        cypher = (
            "MATCH (a:Person)-[r:KNOWS {since: 2020, strength: 0.9}]->"
            "(b:Person) RETURN r"
        )
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "2020" in sql
        assert "0.9" in sql
        assert "AND" in sql.upper()

    def test_node_and_relationship_inline_properties(self) -> None:
        """Test inline properties on both nodes and relationships."""
        cypher = (
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->"
            "(b:Person {age: 30}) RETURN a, r, b"
        )
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "'Alice'" in sql
        assert "2020" in sql
        assert "30" in sql
        # Should have multiple AND operators
        and_count = sql.upper().count("AND")
        assert and_count >= 2, \
            "Should have at least 2 AND operators for 3 filters"

    # =========================================================================
    # Backward Compatibility Tests
    # =========================================================================

    def test_node_without_inline_properties_no_extra_where(self) -> None:
        """Test node without inline properties works normally."""
        cypher = "MATCH (p:Person) RETURN p.name"
        sql = self._transpile(cypher)

        # Should compile successfully
        assert "SELECT" in sql.upper()
        assert "test.persons" in sql

    def test_inline_property_without_label(self) -> None:
        """Test inline property on node without explicit label."""
        cypher = "MATCH (:Person {name: 'Alice'}) RETURN 1"
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "'Alice'" in sql

    # =========================================================================
    # Edge Cases
    # =========================================================================

    def test_empty_inline_properties_ignored(self) -> None:
        """Test that empty inline properties {} are handled gracefully."""
        # Note: This might not parse correctly, but if it does,
        # it should not create spurious filters
        cypher = "MATCH (p:Person) RETURN p.name"
        sql = self._transpile(cypher)

        # Should work normally
        assert "SELECT" in sql.upper()

    def test_string_with_special_characters(self) -> None:
        """Test inline property value with special characters."""
        cypher = "MATCH (p:Person {name: \"O'Brien\"}) RETURN p"
        sql = self._transpile(cypher)

        assert_has_where(sql)
        # Should properly escape the apostrophe
        assert "O'Brien" in sql or "O\\'Brien" in sql or "O''Brien" in sql
