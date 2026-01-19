"""Test 33: Ordered COLLECT.

Validates ordered COLLECT transpilation from Cypher to Databricks SQL.

Cypher ordered COLLECT maps to Databricks ARRAY_SORT + TRANSFORM:
- COLLECT(x ORDER BY y) -> TRANSFORM(ARRAY_SORT(COLLECT_LIST(STRUCT(y, x)), ...), s -> s._value)
- COLLECT(x ORDER BY y DESC) -> with DESC comparator

Fraud Use Cases:
- Collect transactions in chronological order
- Get latest N transactions per account
- Ordered risk factor collection
"""

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


class TestOrderedCollect:
    """Test ordered COLLECT transpilation."""

    TEST_ID = "33"
    TEST_NAME = "ordered_collect"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                    EntityProperty("risk_score", float),
                ],
                node_id_property=EntityProperty("id", str),
            )
        )

        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                    EntityProperty("risk_score", float),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_collect_order_by_asc(self) -> None:
        """Test COLLECT with ORDER BY ascending."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount ORDER BY a.timestamp) AS orderedAmounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ARRAY_SORT" in sql_upper, "Should use ARRAY_SORT"
        assert "TRANSFORM" in sql_upper, "Should use TRANSFORM to extract value"
        assert "STRUCT" in sql_upper, "Should use STRUCT for sort key + value"

    def test_collect_order_by_desc(self) -> None:
        """Test COLLECT with ORDER BY descending."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount ORDER BY a.timestamp DESC) AS latestFirst
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ARRAY_SORT" in sql_upper
        assert "TRANSFORM" in sql_upper
        # DESC should have > comparison for -1 (comes first)
        assert "->" in sql  # Lambda arrow

    def test_collect_order_by_property(self) -> None:
        """Test COLLECT ordering by a different property."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount ORDER BY a.risk_score DESC) AS byRisk
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ARRAY_SORT" in sql_upper
        assert "STRUCT" in sql_upper

    def test_fraud_chronological_amounts(self) -> None:
        """Test fraud scenario: get amounts in chronological order."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount ORDER BY a.timestamp) AS chronological
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ARRAY_SORT" in sql_upper

    def test_fraud_highest_risk_first(self) -> None:
        """Test fraud scenario: order by risk score descending."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.name ORDER BY a.risk_score DESC) AS highRiskFirst
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ARRAY_SORT" in sql_upper
        assert "TRANSFORM" in sql_upper

    def test_collect_without_order_unchanged(self) -> None:
        """Test that regular COLLECT without ORDER BY is unchanged."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount) AS amounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COLLECT_LIST" in sql_upper
        # Should NOT have ARRAY_SORT for non-ordered collect
        assert "ARRAY_SORT" not in sql_upper

    def test_multiple_collects_mixed(self) -> None:
        """Test multiple COLLECTs, some ordered some not."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount) AS unordered,
               COLLECT(a.amount ORDER BY a.timestamp DESC) AS ordered
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have both COLLECT_LIST and ARRAY_SORT
        assert "COLLECT_LIST" in sql_upper
        assert "ARRAY_SORT" in sql_upper

    def test_collect_order_by_same_field(self) -> None:
        """Test COLLECT ordering by the same field being collected."""
        cypher = """
        MATCH (a:Account)
        RETURN COLLECT(a.amount ORDER BY a.amount DESC) AS sortedAmounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ARRAY_SORT" in sql_upper
        assert "STRUCT" in sql_upper
