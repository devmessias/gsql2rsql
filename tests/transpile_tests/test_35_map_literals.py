"""Test 35: Map literals.

Validates map literal transpilation from Cypher to Databricks SQL.

Cypher map literals map to Databricks SQL STRUCT:
- {name: 'John', age: 30} -> STRUCT('John' AS name, 30 AS age)

Fraud Use Cases:
- Create structured responses with multiple fields
- Build aggregated summaries as structs
- Construct date/time from components
- Return complex nested data
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestMapLiterals:
    """Test map literal transpilation."""

    TEST_ID = "35"
    TEST_NAME = "map_literals"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("balance", float),
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
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_simple_map_literal(self) -> None:
        """Test simple map literal with string and number."""
        cypher = """
        MATCH (a:Account)
        RETURN {name: a.name, balance: a.balance} AS summary
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "AS NAME" in sql_upper
        assert "AS BALANCE" in sql_upper

    def test_map_literal_with_constant(self) -> None:
        """Test map literal with constant values."""
        cypher = """
        MATCH (a:Account)
        RETURN {status: 'active', count: 1} AS metadata
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "AS STATUS" in sql_upper
        assert "AS COUNT" in sql_upper

    def test_map_literal_with_expression(self) -> None:
        """Test map literal with computed expressions."""
        cypher = """
        MATCH (a:Account)
        RETURN {doubled: a.balance * 2, isRisky: a.risk_score > 50} AS computed
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "AS DOUBLED" in sql_upper
        assert "AS ISRISKY" in sql_upper

    def test_empty_map_literal(self) -> None:
        """Test empty map literal {}."""
        cypher = """
        MATCH (a:Account)
        RETURN {} AS emptyMap
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT()" in sql_upper

    def test_fraud_account_summary(self) -> None:
        """Test fraud scenario: create account summary struct."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, {
            accountName: a.name,
            currentBalance: a.balance,
            riskLevel: a.risk_score
        } AS accountSummary
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "AS ACCOUNTNAME" in sql_upper
        assert "AS CURRENTBALANCE" in sql_upper
        assert "AS RISKLEVEL" in sql_upper

    def test_map_with_aggregation(self) -> None:
        """Test map literal containing aggregation."""
        cypher = """
        MATCH (a:Account)
        RETURN {
            totalBalance: SUM(a.balance),
            accountCount: COUNT(*)
        } AS summary
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "SUM(" in sql_upper
        assert "COUNT(" in sql_upper

    def test_fraud_risk_profile(self) -> None:
        """Test fraud scenario: build risk profile struct."""
        cypher = """
        MATCH (a:Account)
        WHERE a.risk_score > 70
        RETURN a.id, {
            name: a.name,
            risk: a.risk_score,
            flagged: true
        } AS riskProfile
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "AS RISK" in sql_upper
        assert "AS FLAGGED" in sql_upper

    def test_map_single_field(self) -> None:
        """Test map literal with single field."""
        cypher = """
        MATCH (a:Account)
        RETURN {accountId: a.id} AS wrapper
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "AS ACCOUNTID" in sql_upper

    def test_fraud_transaction_metadata(self) -> None:
        """Test fraud scenario: structured transaction metadata."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, {
            processedBy: 'fraud_system',
            version: 1,
            isManualReview: false
        } AS metadata
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "STRUCT(" in sql_upper
        assert "'FRAUD_SYSTEM'" in sql_upper or "FRAUD_SYSTEM" in sql_upper
        assert "AS PROCESSEDBY" in sql_upper
        assert "AS VERSION" in sql_upper

    def test_date_from_map_in_return(self) -> None:
        """Test date construction using map literal."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, date({year: 2024, month: 6, day: 15}) AS createdDate
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MAKE_DATE" in sql_upper
        assert "2024" in sql

    def test_datetime_from_map_in_return(self) -> None:
        """Test datetime construction using map literal."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, datetime({year: 2024, month: 6, day: 15, hour: 14}) AS ts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MAKE_TIMESTAMP" in sql_upper
        assert "2024" in sql
        assert "14" in sql
