"""Test 26: UNWIND cartesian product (multiple UNWINDs).

Validates that multiple UNWIND clauses produce a cartesian product,
with each UNWIND adding another EXPLODE TVF in the FROM clause.

Cypher:
    UNWIND list1 AS x
    UNWIND list2 AS y
    → produces cartesian product of list1 × list2

Databricks SQL:
    FROM source,
    EXPLODE(list1) AS _exploded1(x),
    EXPLODE(list2) AS _exploded2(y)

Fraud Use Cases:
- Cross-reference multiple watchlists
- Generate all transaction pairs for comparison
- Expand multi-dimensional risk factors
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

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql


class TestUnwindCartesian:
    """Test UNWIND cartesian product transpilation."""

    TEST_ID = "26"
    TEST_NAME = "unwind_cartesian"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("tags", list),
                    EntityProperty("transactionIds", list),
                    EntityProperty("riskFactors", list),
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

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        RETURN a.id, tag, txId
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql ID=26 NAME=unwind_cartesian"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_two_unwinds_produce_two_explodes(self) -> None:
        """Test that two UNWINDs produce two EXPLODE functions."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        RETURN a.id, tag, txId
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        explode_count = sql_upper.count("EXPLODE(")
        assert explode_count >= 2, \
            f"Two UNWINDs should produce >= 2 EXPLODEs, found {explode_count}"

    def test_three_unwinds_produce_three_explodes(self) -> None:
        """Test that three UNWINDs produce three EXPLODE functions."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        UNWIND a.riskFactors AS risk
        RETURN a.id, tag, txId, risk
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        explode_count = sql_upper.count("EXPLODE(")
        assert explode_count >= 3, \
            f"Three UNWINDs should produce >= 3 EXPLODEs, found {explode_count}"

    def test_cartesian_with_literal_arrays(self) -> None:
        """Test cartesian product with literal arrays."""
        cypher = """
        MATCH (a:Account)
        UNWIND [1, 2] AS x
        UNWIND ['a', 'b'] AS y
        RETURN a.id, x, y
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        explode_count = sql_upper.count("EXPLODE(")
        assert explode_count >= 2
        # Should contain all literal values
        assert "1" in sql and "2" in sql
        assert "'A'" in sql_upper or "'a'" in sql.lower()

    def test_cartesian_preserves_source_columns(self) -> None:
        """Test that cartesian product preserves source columns."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        RETURN a.id, a.name, tag, txId
        """
        sql = self._transpile(cypher)

        sql_lower = sql.lower()
        assert "id" in sql_lower, "Should preserve id column"
        assert "name" in sql_lower, "Should preserve name column"

    def test_cartesian_no_lateral_keyword(self) -> None:
        """Test that cartesian uses TVF syntax without deprecated LATERAL."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        RETURN a.id, tag, txId
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Note: LATERAL VIEW is deprecated, but LATERAL with TVFs is modern (Databricks 12.2+)
        assert "LATERAL VIEW" not in sql_upper, \
            "Should NOT use deprecated LATERAL VIEW syntax"

    def test_fraud_cross_reference_watchlists(self) -> None:
        """Test fraud scenario: cross-reference multiple arrays.

        Pattern: Compare each tag with each risk factor.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.riskFactors AS risk
        RETURN a.id, tag, risk
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert sql_upper.count("EXPLODE(") >= 2

    def test_cartesian_with_filter_after(self) -> None:
        """Test cartesian product with filter on combined results."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.riskFactors AS risk
        WITH a, tag, risk
        WHERE tag = risk
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert sql_upper.count("EXPLODE(") >= 2
        # Should have a comparison/filter
        assert "=" in sql or "WHERE" in sql_upper
