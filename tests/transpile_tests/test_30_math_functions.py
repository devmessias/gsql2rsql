"""Test 30: Math functions.

Validates mathematical functions transpilation from Cypher to Databricks SQL.

Cypher math functions map directly to Databricks SQL equivalents:
- abs(x) -> ABS(x)
- ceil(x) -> CEIL(x)
- floor(x) -> FLOOR(x)
- round(x) -> ROUND(x)
- round(x, n) -> ROUND(x, n)
- sqrt(x) -> SQRT(x)
- sign(x) -> SIGN(x)
- log(x) -> LN(x)  # Natural log
- log10(x) -> LOG10(x)
- exp(x) -> EXP(x)
- sin(x) -> SIN(x)
- cos(x) -> COS(x)
- tan(x) -> TAN(x)
- rand() -> RAND()
- pi() -> PI()
- e() -> E()

Fraud Use Cases:
- Calculate absolute differences between expected and actual amounts
- Round transaction amounts for reporting
- Compute statistical measures
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


class TestMathFunctions:
    """Test math function transpilation."""

    TEST_ID = "30"
    TEST_NAME = "math_functions"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("balance", float),
                    EntityProperty("score", float),
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
                    EntityProperty("balance", float),
                    EntityProperty("score", float),
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

    def test_abs_function(self) -> None:
        """Test ABS function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, abs(a.balance) AS absBalance
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ABS(" in sql_upper, "Should use ABS function"

    def test_ceil_function(self) -> None:
        """Test CEIL function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, ceil(a.balance) AS ceilBalance
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "CEIL(" in sql_upper, "Should use CEIL function"

    def test_floor_function(self) -> None:
        """Test FLOOR function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, floor(a.balance) AS floorBalance
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "FLOOR(" in sql_upper, "Should use FLOOR function"

    def test_round_function(self) -> None:
        """Test ROUND function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, round(a.balance) AS roundBalance
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ROUND(" in sql_upper, "Should use ROUND function"

    def test_round_with_precision(self) -> None:
        """Test ROUND function with precision."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, round(a.balance, 2) AS roundBalance
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ROUND(" in sql_upper, "Should use ROUND function"
        assert "2" in sql, "Should have precision parameter"

    def test_sqrt_function(self) -> None:
        """Test SQRT function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, sqrt(a.score) AS sqrtScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SQRT(" in sql_upper, "Should use SQRT function"

    def test_sign_function(self) -> None:
        """Test SIGN function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, sign(a.balance) AS signBalance
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SIGN(" in sql_upper, "Should use SIGN function"

    def test_log_function(self) -> None:
        """Test LOG (natural log) function.

        Cypher log() maps to Databricks LN().
        """
        cypher = """
        MATCH (a:Account)
        RETURN a.id, log(a.score) AS logScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "LN(" in sql_upper, "Should use LN function for natural log"

    def test_log10_function(self) -> None:
        """Test LOG10 function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, log10(a.score) AS log10Score
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "LOG10(" in sql_upper, "Should use LOG10 function"

    def test_exp_function(self) -> None:
        """Test EXP function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, exp(a.score) AS expScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXP(" in sql_upper, "Should use EXP function"

    def test_sin_function(self) -> None:
        """Test SIN function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, sin(a.score) AS sinScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SIN(" in sql_upper, "Should use SIN function"

    def test_cos_function(self) -> None:
        """Test COS function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, cos(a.score) AS cosScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COS(" in sql_upper, "Should use COS function"

    def test_tan_function(self) -> None:
        """Test TAN function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, tan(a.score) AS tanScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "TAN(" in sql_upper, "Should use TAN function"

    def test_rand_function(self) -> None:
        """Test RAND function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, rand() AS randomValue
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "RAND()" in sql_upper, "Should use RAND function"

    def test_pi_function(self) -> None:
        """Test PI function."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, pi() AS piValue
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "PI()" in sql_upper, "Should use PI function"

    def test_e_function(self) -> None:
        """Test E function (Euler's number)."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, e() AS eulerValue
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "E()" in sql_upper, "Should use E function"

    def test_combined_math_expression(self) -> None:
        """Test combined math expression."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, abs(a.balance) + sqrt(a.score) AS combined
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ABS(" in sql_upper
        assert "SQRT(" in sql_upper

    def test_nested_math_functions(self) -> None:
        """Test nested math functions."""
        cypher = """
        MATCH (a:Account)
        RETURN a.id, round(sqrt(abs(a.balance)), 2) AS nested
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ROUND(" in sql_upper
        assert "SQRT(" in sql_upper
        assert "ABS(" in sql_upper

    def test_fraud_abs_difference(self) -> None:
        """Test fraud scenario: calculate absolute difference."""
        cypher = """
        MATCH (a:Account)
        WHERE abs(a.balance - 10000) < 100
        RETURN a.id
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "ABS(" in sql_upper
