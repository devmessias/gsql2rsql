"""Tests for inline property filters in node patterns.

This test module validates that the parser correctly extracts inline
property filters from node patterns like (n:Person {name: 'Alice'})
and stores them in the NodeEntity AST.

Following TDD: These tests are written FIRST and will FAIL until
the parser is modified to extract inline properties.
"""

from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.parser.ast import (
    NodeEntity,
    RelationshipEntity,
    QueryExpressionMapLiteral,
    QueryExpressionValue,
)


class TestParserInlineProperties:
    """Test suite for inline property filter parsing."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

    def test_parse_node_with_single_inline_property(self):
        """Parser should extract single inline property from node pattern.

        Test Case: (a:Person {name: 'Alice'})
        Expected: NodeEntity with inline_properties containing one entry
        """
        cypher = "MATCH (a:Person {name: 'Alice'}) RETURN a"
        ast = self.parser.parse(cypher)

        # Navigate to NodeEntity
        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        assert isinstance(node, NodeEntity)
        assert node.alias == "a"
        assert node.entity_name == "Person"

        # THIS WILL FAIL - inline_properties doesn't exist yet
        assert node.inline_properties is not None, \
            "inline_properties field missing from NodeEntity"

        assert isinstance(node.inline_properties, QueryExpressionMapLiteral), \
            "inline_properties should be QueryExpressionMapLiteral"

        assert len(node.inline_properties.entries) == 1, \
            "Should have exactly one property entry"

        # Check property entry
        prop_name, prop_value = node.inline_properties.entries[0]
        assert prop_name == "name"
        assert isinstance(prop_value, QueryExpressionValue)
        assert prop_value.value == "Alice"
        assert prop_value.value_type == str

    def test_parse_node_with_multiple_inline_properties(self):
        """Parser should extract multiple inline properties.

        Test Case: (a:Person {name: 'Alice', age: 30})
        Expected: NodeEntity with two property entries
        """
        cypher = "MATCH (a:Person {name: 'Alice', age: 30}) RETURN a"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        assert node.inline_properties is not None
        assert len(node.inline_properties.entries) == 2

        # Check first property
        prop1_name, prop1_value = node.inline_properties.entries[0]
        assert prop1_name == "name"
        assert prop1_value.value == "Alice"

        # Check second property
        prop2_name, prop2_value = node.inline_properties.entries[1]
        assert prop2_name == "age"
        assert prop2_value.value == 30
        assert prop2_value.value_type == int

    def test_parse_node_without_inline_properties(self):
        """Parser should handle nodes without inline properties (backward compat).

        Test Case: (a:Person)
        Expected: NodeEntity with inline_properties = None
        """
        cypher = "MATCH (a:Person) RETURN a"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        assert isinstance(node, NodeEntity)
        # inline_properties should be None when not specified
        assert node.inline_properties is None, \
            "inline_properties should be None for nodes without properties"

    def test_parse_multiple_nodes_with_inline_properties(self):
        """Parser should extract inline properties from multiple nodes.

        Test Case: (a {name: 'Alice'})-[r:KNOWS]->(b {age: 30})
        Expected: Both nodes have inline_properties
        """
        cypher = (
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->"
            "(b:Person {age: 30}) RETURN a, b"
        )
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        pattern_parts = match_clause.pattern_parts

        # First node (a:Person {name: 'Alice'})
        node_a = pattern_parts[0]
        assert isinstance(node_a, NodeEntity)
        assert node_a.alias == "a"
        assert node_a.inline_properties is not None
        assert len(node_a.inline_properties.entries) == 1
        assert node_a.inline_properties.entries[0][0] == "name"
        assert node_a.inline_properties.entries[0][1].value == "Alice"

        # Third element is the target node (b:Person {age: 30})
        node_b = pattern_parts[2]
        assert isinstance(node_b, NodeEntity)
        assert node_b.alias == "b"
        assert node_b.inline_properties is not None
        assert len(node_b.inline_properties.entries) == 1
        assert node_b.inline_properties.entries[0][0] == "age"
        assert node_b.inline_properties.entries[0][1].value == 30

    def test_parse_inline_property_with_boolean_value(self):
        """Parser should handle boolean values in inline properties.

        Test Case: (a:Person {active: true})
        Expected: Boolean value correctly parsed
        """
        cypher = "MATCH (a:Person {active: true}) RETURN a"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        assert node.inline_properties is not None
        prop_name, prop_value = node.inline_properties.entries[0]
        assert prop_name == "active"
        assert prop_value.value is True
        assert prop_value.value_type == bool

    def test_parse_inline_property_with_null_value(self):
        """Parser should handle null values in inline properties.

        Test Case: (a:Person {phone: null})
        Expected: None value correctly parsed
        """
        cypher = "MATCH (a:Person {phone: null}) RETURN a"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        assert node.inline_properties is not None
        prop_name, prop_value = node.inline_properties.entries[0]
        assert prop_name == "phone"
        assert prop_value.value is None

    def test_parse_node_with_label_and_inline_properties(self):
        """Parser should extract both label and inline properties.

        Test Case: (a:Person {name: 'Alice'})
        Expected: Both entity_name and inline_properties populated
        """
        cypher = "MATCH (a:Person {name: 'Alice'}) RETURN a"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        # Both should be present
        assert node.entity_name == "Person"
        assert node.inline_properties is not None
        assert len(node.inline_properties.entries) == 1

    def test_parse_node_without_variable_but_with_inline_properties(self):
        """Parser should handle nodes without variable but with properties.

        Test Case: (:Person {name: 'Alice'})
        Expected: Empty alias but inline_properties populated
        """
        cypher = "MATCH (:Person {name: 'Alice'}) RETURN *"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        node = match_clause.pattern_parts[0]

        assert node.alias == ""  # No variable
        assert node.entity_name == "Person"
        assert node.inline_properties is not None
        assert len(node.inline_properties.entries) == 1

    # =================================================================
    # Relationship Inline Property Tests
    # =================================================================

    def test_parse_relationship_with_single_inline_property(self):
        """Parser should extract inline property from relationship pattern.

        Test Case: -[r:KNOWS {since: 2020}]->
        Expected: RelationshipEntity with inline_properties
        """
        cypher = "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN r"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        rel = match_clause.pattern_parts[1]

        assert isinstance(rel, RelationshipEntity)
        assert rel.alias == "r"
        assert rel.entity_name == "KNOWS"

        # Should extract inline properties
        assert rel.inline_properties is not None, \
            "inline_properties field missing from RelationshipEntity"

        assert isinstance(
            rel.inline_properties, QueryExpressionMapLiteral
        ), "inline_properties should be QueryExpressionMapLiteral"

        assert len(rel.inline_properties.entries) == 1
        prop_name, prop_value = rel.inline_properties.entries[0]
        assert prop_name == "since"
        assert prop_value.value == 2020
        assert prop_value.value_type == int

    def test_parse_relationship_with_multiple_inline_properties(self):
        """Parser should extract multiple inline properties from rel.

        Test Case: -[r:KNOWS {since: 2020, strength: 0.9}]->
        Expected: RelationshipEntity with two property entries
        """
        cypher = (
            "MATCH (a)-[r:KNOWS {since: 2020, strength: 0.9}]->(b) "
            "RETURN r"
        )
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        rel = match_clause.pattern_parts[1]

        assert rel.inline_properties is not None
        assert len(rel.inline_properties.entries) == 2

        # Check first property
        prop1_name, prop1_value = rel.inline_properties.entries[0]
        assert prop1_name == "since"
        assert prop1_value.value == 2020

        # Check second property
        prop2_name, prop2_value = rel.inline_properties.entries[1]
        assert prop2_name == "strength"
        assert prop2_value.value == 0.9
        assert prop2_value.value_type == float

    def test_parse_relationship_without_inline_properties(self):
        """Parser should handle relationships without inline properties.

        Test Case: -[r:KNOWS]->
        Expected: RelationshipEntity with inline_properties = None
        """
        cypher = "MATCH (a)-[r:KNOWS]->(b) RETURN r"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        rel = match_clause.pattern_parts[1]

        assert isinstance(rel, RelationshipEntity)
        # inline_properties should be None when not specified
        assert rel.inline_properties is None, \
            "inline_properties should be None for rels without properties"

    def test_parse_pattern_with_node_and_rel_inline_properties(self):
        """Parser should extract inline properties from both nodes and rels.

        Test Case: (a {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b {age: 30})
        Expected: All entities have inline_properties
        """
        cypher = (
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->"
            "(b:Person {age: 30}) RETURN a, r, b"
        )
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        pattern_parts = match_clause.pattern_parts

        # Node a
        node_a = pattern_parts[0]
        assert isinstance(node_a, NodeEntity)
        assert node_a.inline_properties is not None
        assert node_a.inline_properties.entries[0][0] == "name"
        assert node_a.inline_properties.entries[0][1].value == "Alice"

        # Relationship r
        rel_r = pattern_parts[1]
        assert isinstance(rel_r, RelationshipEntity)
        assert rel_r.inline_properties is not None
        assert rel_r.inline_properties.entries[0][0] == "since"
        assert rel_r.inline_properties.entries[0][1].value == 2020

        # Node b
        node_b = pattern_parts[2]
        assert isinstance(node_b, NodeEntity)
        assert node_b.inline_properties is not None
        assert node_b.inline_properties.entries[0][0] == "age"
        assert node_b.inline_properties.entries[0][1].value == 30

    def test_parse_relationship_without_variable_with_inline_properties(
        self
    ):
        """Parser should handle rel without variable but with properties.

        Test Case: -[:KNOWS {since: 2020}]->
        Expected: Empty alias but inline_properties populated
        """
        cypher = "MATCH (a)-[:KNOWS {since: 2020}]->(b) RETURN a, b"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        rel = match_clause.pattern_parts[1]

        assert rel.alias == ""  # No variable
        assert rel.entity_name == "KNOWS"
        assert rel.inline_properties is not None
        assert len(rel.inline_properties.entries) == 1

    def test_parse_variable_length_rel_with_inline_properties(self):
        """Parser should handle variable-length rel with inline properties.

        Test Case: -[r:KNOWS*1..3 {weight: 1.0}]->
        Expected: RelationshipEntity with min_hops, max_hops, and properties
        """
        cypher = "MATCH (a)-[r:KNOWS*1..3 {weight: 1.0}]->(b) RETURN r"
        ast = self.parser.parse(cypher)

        match_clause = ast.parts[0].match_clauses[0]
        rel = match_clause.pattern_parts[1]

        assert isinstance(rel, RelationshipEntity)
        assert rel.alias == "r"
        assert rel.entity_name == "KNOWS"
        assert rel.min_hops == 1
        assert rel.max_hops == 3
        assert rel.is_variable_length is True

        # Should also extract inline properties
        assert rel.inline_properties is not None
        assert len(rel.inline_properties.entries) == 1
        prop_name, prop_value = rel.inline_properties.entries[0]
        assert prop_name == "weight"
        assert prop_value.value == 1.0
        assert prop_value.value_type == float
