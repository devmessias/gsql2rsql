"""Test 43: Bare entity variable in GROUP BY clause.

Validates that when a bare entity variable (like `l`) is used in a WITH clause
with aggregations, it gets properly resolved to its ID column (like `_gsql2rsql_l_id`)
instead of being rendered as a bare variable name.

This is a regression test for the bug where:
  WITH c1, c2, l, AVG(a1.balance) AS avg_bal
produces GROUP BY with bare 'l' instead of '_gsql2rsql_l_id'.
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


class TestBareEntityInGroupBy:
    """Test bare entity variables in GROUP BY clauses."""

    TEST_ID = "43"
    TEST_NAME = "bare_entity_in_group_by"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # SQL schema with Customer, Loan, Account
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Customer",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("status", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Customer"),
        )
        self.schema.add_node(
            NodeSchema(
                name="Loan",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("balance", float),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Loan"),
        )
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("balance", float),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="CO_BORROWER",
                source_node_id="Customer",
                sink_node_id="Loan",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("loan_id", int),
            ),
            SQLTableDescriptor(table_name="graph.CoBorrower"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="HAS_ACCOUNT",
                source_node_id="Customer",
                sink_node_id="Account",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("account_id", int),
            ),
            SQLTableDescriptor(table_name="graph.CustomerAccount"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_simple_bare_entity_in_with(self) -> None:
        """Test basic scenario: bare entity variable in WITH with aggregation.

        This test checks that a bare entity variable like 'l' is properly
        resolved to its ID column in the GROUP BY clause.
        """
        cypher = """
        MATCH (c:Customer)-[:CO_BORROWER]->(l:Loan)
        WITH c, l, COUNT(*) AS loan_count
        RETURN c.id, l.id AS loan_id, loan_count
        """
        sql = self._transpile(cypher)

        # The SQL should NOT contain bare 'l' or 'c' in GROUP BY
        # Instead it should have _gsql2rsql_c_id and _gsql2rsql_l_id
        assert ", l," not in sql.lower(), f"Found bare 'l' in SQL:\n{sql}"
        assert ", c," not in sql.lower(), f"Found bare 'c' in SQL:\n{sql}"

        # Should contain the proper column references
        assert "_gsql2rsql_l_id" in sql.lower() or "_gsql2rsql_l_" in sql.lower(), f"Missing _gsql2rsql_l_ reference in SQL:\n{sql}"
        assert "_gsql2rsql_c_id" in sql.lower() or "_gsql2rsql_c_" in sql.lower(), f"Missing _gsql2rsql_c_ reference in SQL:\n{sql}"

    def test_coborrower_scenario(self) -> None:
        """Test the exact co-borrower scenario that triggered the bug.

        Query pattern:
        MATCH (c1:Customer)-[:CO_BORROWER]->(l:Loan)<-[:CO_BORROWER]-(c2:Customer)
        WHERE c1.id < c2.id
        MATCH (c1)-[:HAS_ACCOUNT]->(a1:Account), (c2)-[:HAS_ACCOUNT]->(a2:Account)
        WITH c1, c2, l,
             AVG(a1.balance) AS c1_avg_balance,
             AVG(a2.balance) AS c2_avg_balance
        RETURN ...
        """
        cypher = """
        MATCH (c1:Customer)-[:CO_BORROWER]->(l:Loan)<-[:CO_BORROWER]-(c2:Customer)
        WHERE c1.id < c2.id
        MATCH (c1)-[:HAS_ACCOUNT]->(a1:Account), (c2)-[:HAS_ACCOUNT]->(a2:Account)
        WITH c1, c2, l,
             AVG(a1.balance) AS c1_avg_balance,
             AVG(a2.balance) AS c2_avg_balance
        RETURN c1.id, c2.id, l.id AS loan_id, l.balance,
               c1_avg_balance, c2_avg_balance,
               (c1_avg_balance + c2_avg_balance) AS combined_liquidity
        ORDER BY combined_liquidity DESC
        """
        sql = self._transpile(cypher)

        # GROUP BY should NOT contain bare variable names
        # Check that 'l' doesn't appear as a standalone identifier
        import re
        # This pattern looks for ', l,' or 'BY l,' or similar - bare 'l' used as column
        bare_l_pattern = r'\b(?:BY|,)\s+l\s*(?:,|$)'
        assert not re.search(bare_l_pattern, sql, re.IGNORECASE), \
            f"Found bare 'l' variable in GROUP BY:\n{sql}"

        # Verify proper column references exist
        assert "_gsql2rsql_l_id" in sql or "_gsql2rsql_l_" in sql, f"Missing _gsql2rsql_l_ column reference:\n{sql}"

    def test_multiple_bare_entities_in_with(self) -> None:
        """Test multiple bare entity variables in WITH clause."""
        cypher = """
        MATCH (c:Customer)-[:HAS_ACCOUNT]->(a:Account)
        WITH c, a, SUM(a.balance) AS total
        RETURN c.id, a.id, total
        """
        sql = self._transpile(cypher)

        # Should not have bare variable names in GROUP BY
        assert " c," not in sql or "__c" in sql, f"Found bare 'c' in SQL:\n{sql}"
        assert " a," not in sql or "__a" in sql, f"Found bare 'a' in SQL:\n{sql}"
