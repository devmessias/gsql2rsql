"""Test for multi-WITH entity continuation bug (TypeError in test 16).

This test reproduces the TypeError: expected string or bytes-like object,
got 'SingleQueryNode' that occurs when processing queries with multiple
WITH clauses and entity continuation.
"""

from __future__ import annotations

import pytest

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.planner.subquery_optimizer import optimize_plan
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


@pytest.fixture
def fraud_schema_provider():
    """Create a minimal schema provider for fraud detection patterns."""
    provider = SimpleSQLSchemaProvider()

    # Customer node
    customer_schema = NodeSchema(
        name="Customer",
        properties=[
            EntityProperty(property_name="name", data_type=str),
            EntityProperty(property_name="status", data_type=str),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        customer_schema, SQLTableDescriptor(table_or_view_name="Customer")
    )

    # Card node
    card_schema = NodeSchema(
        name="Card",
        properties=[
            EntityProperty(property_name="number", data_type=str),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        card_schema, SQLTableDescriptor(table_or_view_name="Card")
    )

    # Transaction node
    transaction_schema = NodeSchema(
        name="Transaction",
        properties=[
            EntityProperty(property_name="amount", data_type=float),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        transaction_schema,
        SQLTableDescriptor(table_or_view_name="Transaction"),
    )

    # HAS_CARD edge
    has_card_edge = EdgeSchema(
        name="HAS_CARD",
        source_node_id="Customer",
        sink_node_id="Card",
        source_id_property=EntityProperty(
            property_name="customer_id", data_type=int
        ),
        sink_id_property=EntityProperty(
            property_name="card_id", data_type=int
        ),
        properties=[],
    )
    provider.add_edge(
        has_card_edge, SQLTableDescriptor(table_or_view_name="HAS_CARD")
    )

    # USED_IN edge
    used_in_edge = EdgeSchema(
        name="USED_IN",
        source_node_id="Card",
        sink_node_id="Transaction",
        source_id_property=EntityProperty(
            property_name="card_id", data_type=int
        ),
        sink_id_property=EntityProperty(
            property_name="transaction_id", data_type=int
        ),
        properties=[],
    )
    provider.add_edge(
        used_in_edge, SQLTableDescriptor(table_or_view_name="USED_IN")
    )

    return provider


class TestMultiWithEntityContinuation:
    """Test cases for multi-WITH patterns with entity continuation."""

    def test_double_with_entity_continuation_simple(
        self, fraud_schema_provider
    ):
        """Test simple case: two WITH clauses with entity continuation.

        This is a minimal reproduction of the TypeError in test 16.
        Pattern:
        - MATCH with entities
        - WITH + COLLECT (first aggregation boundary)
        - MATCH using entity from first WITH (entity continuation)
        - WITH + COUNT (second aggregation boundary)
        - RETURN
        """
        cypher = """
        MATCH (c1:Customer)-[:HAS_CARD]->(card:Card)<-[:HAS_CARD]-(c2:Customer)
        WHERE c1.status = 'blacklisted' AND c2.status = 'verified'
        WITH card, COLLECT(c1.id) AS c1_ids
        MATCH (card)-[:USED_IN]->(t:Transaction)
        WITH card, c1_ids, COUNT(t) AS tx_count
        RETURN card.number, SIZE(c1_ids), tx_count
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)

        # This should NOT raise TypeError
        plan = LogicalPlan.process_query_tree(ast, fraud_schema_provider)
        optimize_plan(plan)
        plan.resolve(cypher)

        renderer = SQLRenderer(fraud_schema_provider)
        sql = renderer.render_plan(plan)

        assert sql is not None
        assert "card" in sql.lower()

    def test_double_with_multiple_entities(self, fraud_schema_provider):
        """Test with multiple entities continuing across boundaries."""
        cypher = """
        MATCH (c:Customer)-[:HAS_CARD]->(card:Card)
        WITH c, card, COUNT(*) AS rel_count
        MATCH (card)-[:USED_IN]->(t:Transaction)
        WITH c, card, rel_count, COUNT(t) AS tx_count
        RETURN c.name, card.number, rel_count, tx_count
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)

        plan = LogicalPlan.process_query_tree(ast, fraud_schema_provider)
        optimize_plan(plan)
        plan.resolve(cypher)

        renderer = SQLRenderer(fraud_schema_provider)
        sql = renderer.render_plan(plan)

        assert sql is not None

    def test_original_failing_query_simplified(self, fraud_schema_provider):
        """Test simplified version of original failing query from test 16.

        Original query has COLLECT DISTINCT in first WITH, then MATCH,
        then COUNT/SUM in second WITH.
        """
        cypher = """
        MATCH (blacklisted:Customer)-[:HAS_CARD]->(card:Card)<-[:HAS_CARD]-(verified:Customer)
        WHERE blacklisted.status = 'blacklisted' AND verified.status = 'verified'
        WITH card,
             COLLECT(DISTINCT blacklisted.id) AS blacklisted_customers,
             COLLECT(DISTINCT verified.id) AS verified_customers
        MATCH (card)-[:USED_IN]->(t:Transaction)
        WITH card,
             blacklisted_customers,
             verified_customers,
             COUNT(t) AS total_transactions,
             SUM(t.amount) AS total_amount
        RETURN card.number,
               SIZE(blacklisted_customers) AS blacklisted_count,
               SIZE(verified_customers) AS verified_count,
               total_transactions,
               total_amount
        ORDER BY total_amount DESC
        LIMIT 25
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)

        # This is where the TypeError occurred
        plan = LogicalPlan.process_query_tree(ast, fraud_schema_provider)
        optimize_plan(plan)
        plan.resolve(cypher)

        renderer = SQLRenderer(fraud_schema_provider)
        sql = renderer.render_plan(plan)

        assert sql is not None
        assert "card" in sql.lower()
