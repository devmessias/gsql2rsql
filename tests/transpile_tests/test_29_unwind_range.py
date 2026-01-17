"""Test 29: UNWIND RANGE for numeric sequences.

Validates UNWIND with RANGE function to generate numeric sequences.

Cypher:
    UNWIND RANGE(start, end) AS i
    UNWIND RANGE(start, end, step) AS i

Databricks SQL:
    EXPLODE(SEQUENCE(start, end)) AS t(i)
    EXPLODE(SEQUENCE(start, end, step)) AS t(i)

Note: Both Cypher RANGE and Databricks SEQUENCE are inclusive on both ends.
RANGE(0, 5) → [0, 1, 2, 3, 4, 5]
SEQUENCE(0, 5) → [0, 1, 2, 3, 4, 5]

Fraud Use Cases:
- Generate time windows for analysis
- Create index sequences for array operations
- Batch processing with offset iterations
- Generate date ranges for transaction windows
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

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql


class TestUnwindRange:
    """Test UNWIND RANGE transpilation."""

    TEST_ID = "29"
    TEST_NAME = "unwind_range"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("batchSize", int),
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
                    EntityProperty("batchSize", int),
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

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 5) AS idx
        RETURN a.id, idx
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql ID=29 NAME=unwind_range"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_range_maps_to_sequence(self) -> None:
        """Test that RANGE maps to Databricks SEQUENCE function."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 5) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "SEQUENCE" in sql_upper, \
            "RANGE should map to SEQUENCE in Databricks"

    def test_range_with_start_end(self) -> None:
        """Test basic RANGE(start, end) syntax."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(1, 10) AS i
        RETURN a.id, i
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE(1, 10)" in sql_upper or "SEQUENCE(1,10)" in sql_upper.replace(" ", "")

    def test_range_with_zero_start(self) -> None:
        """Test RANGE starting from zero."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 3) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        assert "0" in sql
        assert "3" in sql

    def test_range_with_step(self) -> None:
        """Test RANGE with step parameter.

        RANGE(0, 10, 2) → [0, 2, 4, 6, 8, 10]
        """
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 10, 2) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE" in sql_upper
        # Should have all three parameters
        assert "0" in sql and "10" in sql and "2" in sql

    def test_range_negative_values(self) -> None:
        """Test RANGE with negative values."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(-5, 5) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE" in sql_upper
        # Negative may be rendered as -(5) or -5
        assert "-5" in sql or "-(5)" in sql
        assert "5" in sql

    def test_range_single_value(self) -> None:
        """Test RANGE that produces single value.

        RANGE(5, 5) → [5]
        """
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(5, 5) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE" in sql_upper

    def test_range_with_expressions(self) -> None:
        """Test RANGE with property expressions as bounds."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, a.batchSize) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE" in sql_upper
        # Should reference the property
        assert "BATCHSIZE" in sql_upper or "batch_size" in sql.lower()

    def test_fraud_time_windows(self) -> None:
        """Test fraud scenario: generate time window indices."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 23) AS hour
        RETURN a.id, hour
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE" in sql_upper
        assert "0" in sql and "23" in sql

    def test_fraud_batch_iterations(self) -> None:
        """Test fraud scenario: batch processing iterations."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(1, 100, 10) AS batchStart
        RETURN a.id, batchStart
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "SEQUENCE" in sql_upper

    def test_no_lateral_keyword(self) -> None:
        """Test that RANGE UNWIND doesn't use deprecated LATERAL."""
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 5) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "LATERAL" not in sql_upper
