"""Test 31: List comprehension.

Validates list comprehension transpilation from Cypher to Databricks SQL.

Cypher list comprehensions map to Databricks HOFs:
- [x IN list] -> list (identity)
- [x IN list WHERE pred] -> FILTER(list, x -> pred)
- [x IN list | expr] -> TRANSFORM(list, x -> expr)
- [x IN list WHERE pred | expr] -> TRANSFORM(FILTER(list, x -> pred), x -> expr)

Fraud Use Cases:
- Filter suspicious transactions: [t IN transactions WHERE t.flagged]
- Transform amounts: [t IN transactions | t.amount * 1.1]
- Extract IDs: [t IN transactions WHERE t.amount > 10000 | t.id]
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


class TestListComprehension:
    """Test list comprehension transpilation."""

    TEST_ID = "31"
    TEST_NAME = "list_comprehension"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("amounts", list),
                    EntityProperty("scores", list),
                    EntityProperty("tags", list),
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
                    EntityProperty("amounts", list),
                    EntityProperty("scores", list),
                    EntityProperty("tags", list),
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
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_filter_only(self) -> None:
        """Test list comprehension with filter only.

        [x IN list WHERE x > 0] -> FILTER(list, x -> x > 0)
        """
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [x IN a.amounts WHERE x > 0] AS positiveAmounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FILTER(" in sql_upper, "Should use FILTER HOF"
        assert "->" in sql, "Should have lambda arrow"

    def test_transform_only(self) -> None:
        """Test list comprehension with transform only.

        [x IN list | x * 2] -> TRANSFORM(list, x -> x * 2)
        """
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [x IN a.amounts | x * 2] AS doubledAmounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "TRANSFORM(" in sql_upper, "Should use TRANSFORM HOF"
        assert "->" in sql, "Should have lambda arrow"

    def test_filter_and_transform(self) -> None:
        """Test list comprehension with filter and transform.

        [x IN list WHERE x > 0 | x * 2] -> TRANSFORM(FILTER(list, x -> x > 0), x -> x * 2)
        """
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [x IN a.amounts WHERE x > 0 | x * 2] AS positiveDoubled
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FILTER(" in sql_upper, "Should use FILTER HOF"
        assert "TRANSFORM(" in sql_upper, "Should use TRANSFORM HOF"
        # TRANSFORM should wrap FILTER
        filter_pos = sql_upper.find("FILTER(")
        transform_pos = sql_upper.find("TRANSFORM(")
        assert transform_pos < filter_pos, "TRANSFORM should wrap FILTER"

    def test_nested_comprehension(self) -> None:
        """Test nested list comprehension."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [x IN [y IN a.amounts WHERE y > 0] | x * 2] AS nested
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert sql_upper.count("FILTER(") >= 1 or sql_upper.count("TRANSFORM(") >= 1

    def test_string_filter(self) -> None:
        """Test list comprehension filtering strings."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [t IN a.tags WHERE t <> 'ignore'] AS filteredTags
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FILTER(" in sql_upper

    def test_fraud_suspicious_filter(self) -> None:
        """Test fraud scenario: filter suspicious amounts."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [amt IN a.amounts WHERE amt > 10000] AS largeAmounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FILTER(" in sql_upper

    def test_fraud_transform_amounts(self) -> None:
        """Test fraud scenario: transform amounts with fee."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [amt IN a.amounts | amt * 1.1] AS withFee
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "TRANSFORM(" in sql_upper

    def test_fraud_filter_and_transform(self) -> None:
        """Test fraud scenario: filter large amounts and add flag."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [amt IN a.amounts WHERE amt > 5000 | amt + 100] AS flaggedAmounts
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FILTER(" in sql_upper
        assert "TRANSFORM(" in sql_upper

    def test_comprehension_with_math(self) -> None:
        """Test list comprehension with math functions."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [x IN a.scores | abs(x)] AS absScores
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "TRANSFORM(" in sql_upper
        assert "ABS(" in sql_upper

    def test_comprehension_comparison_operators(self) -> None:
        """Test list comprehension with various comparison operators."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, [x IN a.amounts WHERE x >= 100] AS atLeast100
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FILTER(" in sql_upper
        assert ">=" in sql
