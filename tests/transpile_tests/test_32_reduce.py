"""Test 32: REDUCE expression.

Validates REDUCE transpilation from Cypher to Databricks SQL.

Cypher REDUCE maps to Databricks AGGREGATE HOF:
- REDUCE(acc = init, x IN list | expr) -> AGGREGATE(list, init, (acc, x) -> expr)

Fraud Use Cases:
- Sum transaction amounts: REDUCE(total = 0, x IN amounts | total + x)
- Concatenate strings: REDUCE(s = '', x IN names | s + x)
- Find maximum: REDUCE(max = 0, x IN scores | CASE WHEN x > max THEN x ELSE max END)
- Count elements: REDUCE(cnt = 0, x IN items | cnt + 1)
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


class TestReduce:
    """Test REDUCE expression transpilation."""

    TEST_ID = "32"
    TEST_NAME = "reduce"

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
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_reduce_sum(self) -> None:
        """Test REDUCE for summing array elements.

        REDUCE(total = 0, x IN amounts | total + x)
        -> AGGREGATE(amounts, 0, (total, x) -> total + x)
        """
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(total = 0, x IN a.amounts | total + x) AS totalAmount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper, "Should use AGGREGATE HOF"
        assert "->" in sql, "Should have lambda arrow"

    def test_reduce_count(self) -> None:
        """Test REDUCE for counting array elements."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(cnt = 0, x IN a.amounts | cnt + 1) AS count
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper

    def test_reduce_with_multiplication(self) -> None:
        """Test REDUCE with multiplication."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(product = 1, x IN a.scores | product * x) AS product
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper
        assert "*" in sql

    def test_reduce_fraud_total_risk(self) -> None:
        """Test fraud scenario: calculate total risk score."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(risk = 0, s IN a.scores | risk + s) AS totalRisk
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper

    def test_reduce_with_initial_value(self) -> None:
        """Test REDUCE with non-zero initial value."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(sum = 100, x IN a.amounts | sum + x) AS adjustedSum
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper
        assert "100" in sql

    def test_reduce_with_subtraction(self) -> None:
        """Test REDUCE with subtraction operation."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(balance = 1000, x IN a.amounts | balance - x) AS remaining
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper
        assert "-" in sql

    def test_reduce_nested_in_expression(self) -> None:
        """Test REDUCE nested in a larger expression."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(sum = 0, x IN a.amounts | sum + x) * 2 AS doubledTotal
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper

    def test_reduce_fraud_weighted_score(self) -> None:
        """Test fraud scenario: calculate weighted score (sum * factor)."""
        cypher = """
        MATCH (a:Account)
        WHERE REDUCE(total = 0, x IN a.scores | total + x) > 100
        RETURN a.id
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper
        assert "WHERE" in sql_upper

    def test_reduce_with_abs_function(self) -> None:
        """Test REDUCE with abs function in expression."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(sum = 0, x IN a.amounts | sum + abs(x)) AS absSum
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper
        assert "ABS(" in sql_upper

    def test_reduce_fraud_deviation_sum(self) -> None:
        """Test fraud scenario: sum of absolute deviations."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, REDUCE(dev = 0, x IN a.amounts | dev + abs(x - 1000)) AS deviation
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "AGGREGATE(" in sql_upper
