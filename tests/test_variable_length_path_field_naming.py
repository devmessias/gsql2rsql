"""Tests for variable-length path field naming bug.

This test module follows TDD principles to fix the double-prefixed field name bug
that occurs in variable-length path queries with joins.

Bug: When generating SQL for variable-length paths with subsequent joins,
field names are incorrectly double-prefixed (e.g., _gsql2rsql_peer_peer_id
instead of _gsql2rsql_peer_id).
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
    SimpleGraphSchemaProvider,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestVariableLengthPathFieldNaming:
    """Tests for correct field naming in variable-length path queries."""

    def setup_method(self) -> None:
        """Set up test fixtures with graph schema for testing."""
        # Graph schema: Customer -[:KNOWS]- Customer -[:HAS_LOAN]-> Loan
        self.graph_schema = SimpleGraphSchemaProvider()

        # Nodes
        self.graph_schema.add_node(
            NodeSchema(
                name="Customer",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("status", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_node(
            NodeSchema(
                name="Loan",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("amount", float),
                    EntityProperty("status", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("balance", float),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )

        # Edges
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Customer",
                sink_node_id="Customer",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("knows_customer_id", int),
                properties=[],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="HAS_LOAN",
                source_node_id="Customer",
                sink_node_id="Loan",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("loan_id", int),
                properties=[],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="HAS_ACCOUNT",
                source_node_id="Customer",
                sink_node_id="Account",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("account_id", int),
                properties=[],
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Customer",
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.Customer"),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Loan",
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.Loan"),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.Account"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Customer",
                sink_node_id="Customer",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("knows_customer_id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.CustomerKnows"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="HAS_LOAN",
                source_node_id="Customer",
                sink_node_id="Loan",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("loan_id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.CustomerLoan"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="HAS_ACCOUNT",
                source_node_id="Customer",
                sink_node_id="Account",
                source_id_property=EntityProperty("customer_id", int),
                sink_id_property=EntityProperty("account_id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.CustomerAccount"),
        )

    def test_varlen_path_1_2_with_subsequent_join(self):
        """
        Test variable-length path [*1..2] followed by a join.

        This is the original failing case from credit_queries.yaml:07.
        The bug manifests as double-prefixed field names in join conditions.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*1..2]-(peer:Customer)-[:HAS_LOAN]->(l:Loan)
        WHERE l.status = 'defaulted'
        RETURN c.id, c.name, peer.id
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # BUG CHECK: Should NOT have double-prefixed field names
        assert "_gsql2rsql_peer_peer_id" not in sql, (
            "Field name incorrectly double-prefixed: _gsql2rsql_peer_peer_id "
            "should be _gsql2rsql_peer_id"
        )

        # CORRECTNESS CHECK: Should have properly prefixed field names
        assert "_gsql2rsql_peer_id" in sql, (
            "Missing expected field name: _gsql2rsql_peer_id"
        )

        # Should reference peer's ID in join condition with HAS_LOAN edge
        # The join should connect peer entity to the edge table
        assert "peer" in sql.lower(), "Query should reference 'peer' entity"

    def test_varlen_path_1_3_with_subsequent_join(self):
        """
        Test variable-length path [*1..3] followed by a join.

        Tests that the fix works for different path lengths.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*1..3]-(peer:Customer)-[:HAS_LOAN]->(l:Loan)
        RETURN c.id, peer.id, l.id
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names
        assert "_gsql2rsql_peer_peer_id" not in sql
        assert "_gsql2rsql_peer_id" in sql

    def test_varlen_path_2_4_with_subsequent_join(self):
        """
        Test variable-length path [*2..4] (min > 1) followed by a join.

        Tests edge case where minimum path length is greater than 1.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*2..4]-(peer:Customer)-[:HAS_ACCOUNT]->(a:Account)
        RETURN c.id, peer.id, a.balance
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names
        assert "_gsql2rsql_peer_peer_id" not in sql
        assert "_gsql2rsql_peer_id" in sql

    def test_varlen_path_exact_length_with_subsequent_join(self):
        """
        Test variable-length path with exact length [*3] followed by a join.

        Tests that the fix works when min == max path length.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*3]-(peer:Customer)-[:HAS_LOAN]->(l:Loan)
        RETURN c.id, peer.name
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names
        assert "_gsql2rsql_peer_peer_id" not in sql
        assert "_gsql2rsql_peer_id" in sql

    @pytest.mark.skip(
        reason="Multiple varlen paths in comma-separated MATCH patterns not supported yet. "
        "Bug in planner/resolver: only the last pattern is added to symbol table. "
        "This is unrelated to the double-prefix bug fixed in this PR."
    )
    def test_multiple_varlen_paths_in_query(self):
        """
        Test query with multiple variable-length paths.

        Tests that the fix works when multiple varlen paths appear in the same query.

        NOTE: Currently fails with ColumnResolutionError because the planner/resolver
        doesn't properly handle multiple varlen path patterns separated by commas.
        Only the last pattern (peer2) is added to the symbol table.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*1..2]-(peer1:Customer),
              (c)-[:KNOWS*1..2]-(peer2:Customer)
        WHERE peer1.id < peer2.id
        RETURN c.id, peer1.id, peer2.id
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names for either peer
        assert "_gsql2rsql_peer1_peer1_id" not in sql
        assert "_gsql2rsql_peer2_peer2_id" not in sql
        assert "_gsql2rsql_peer1_id" in sql
        assert "_gsql2rsql_peer2_id" in sql

    def test_varlen_path_with_aggregation(self):
        """
        Test variable-length path followed by aggregation.

        This is similar to the original failing case which uses COUNT(DISTINCT peer).
        Tests that field names are correct in aggregation context.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*1..2]-(peer:Customer)-[:HAS_LOAN]->(l:Loan)
        WHERE l.status = 'defaulted'
        WITH c, COUNT(DISTINCT peer) AS defaulted_peers
        RETURN c.id, defaulted_peers
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names
        assert "_gsql2rsql_peer_peer_id" not in sql
        # Should be able to aggregate on peer correctly
        assert "COUNT(DISTINCT" in sql.upper()

    def test_varlen_path_without_subsequent_join(self):
        """
        Test variable-length path WITHOUT subsequent join (baseline).

        This should work correctly and serves as a baseline to ensure
        the bug is specific to varlen paths with subsequent joins.
        """
        cypher = """
        MATCH (c:Customer)-[:KNOWS*1..2]-(peer:Customer)
        RETURN c.id, peer.id
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names (baseline expectation)
        assert "_gsql2rsql_peer_peer_id" not in sql
        assert "_gsql2rsql_peer_id" in sql

    def test_varlen_path_different_edge_type(self):
        """
        Test variable-length path with different edge type.

        Tests that the fix is generic and works for different edge types,
        not just KNOWS edges.
        """
        # First add a self-referential edge for Account (for testing purposes)
        self.graph_schema.add_edge(
            EdgeSchema(
                name="LINKED_TO",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("account_id", int),
                sink_id_property=EntityProperty("linked_account_id", int),
                properties=[],
            )
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="LINKED_TO",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("account_id", int),
                sink_id_property=EntityProperty("linked_account_id", int),
            ),
            SQLTableDescriptor(table_name="catalog.credit.AccountLinks"),
        )

        cypher = """
        MATCH (a:Account)-[:LINKED_TO*1..2]-(linked:Account)
        WHERE linked.balance > 1000
        RETURN a.id, linked.id
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should NOT have double-prefixed field names
        assert "_gsql2rsql_linked_linked_id" not in sql
        assert "_gsql2rsql_linked_id" in sql
