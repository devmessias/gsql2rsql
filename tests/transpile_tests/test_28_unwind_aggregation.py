"""Test 28: UNWIND with aggregation.

Validates UNWIND followed by aggregation functions, enabling
array transformation and re-grouping patterns.

Common patterns:
- UNWIND + COUNT: Count elements in arrays
- UNWIND + COLLECT: Transform and re-collect arrays
- UNWIND + SUM/AVG: Aggregate array values
- UNWIND + DISTINCT: Get unique array elements

Databricks SQL:
    SELECT id, COUNT(element) AS cnt
    FROM source, EXPLODE(array) AS t(element)
    GROUP BY id

Fraud Use Cases:
- Count high-risk transactions per account
- Calculate average transaction amounts from arrays
- Deduplicate transaction IDs
- Sum risk scores across factors
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


class TestUnwindAggregation:
    """Test UNWIND with aggregation transpilation."""

    TEST_ID = "28"
    TEST_NAME = "unwind_aggregation"

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
                    EntityProperty("scores", list),
                    EntityProperty("amounts", list),
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
        RETURN a.id, COUNT(tag) AS tagCount
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql ID=28 NAME=unwind_aggregation"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_unwind_count(self) -> None:
        """Test UNWIND followed by COUNT aggregation."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, COUNT(tag) AS tagCount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COUNT" in sql_upper
        assert "GROUP BY" in sql_upper or "GROUP" in sql_upper

    def test_unwind_collect(self) -> None:
        """Test UNWIND followed by COLLECT (re-grouping).

        Pattern: Expand, filter, and re-collect.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        WITH a, tag
        WHERE tag <> 'ignore'
        RETURN a.id, COLLECT(tag) AS filteredTags
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COLLECT_LIST" in sql_upper or "ARRAY_AGG" in sql_upper

    def test_unwind_sum(self) -> None:
        """Test UNWIND followed by SUM aggregation."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.scores AS score
        RETURN a.id, SUM(score) AS totalScore
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "SUM" in sql_upper

    def test_unwind_avg(self) -> None:
        """Test UNWIND followed by AVG aggregation."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.amounts AS amount
        RETURN a.id, AVG(amount) AS avgAmount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "AVG" in sql_upper

    def test_unwind_max_min(self) -> None:
        """Test UNWIND followed by MAX/MIN aggregation."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.amounts AS amount
        RETURN a.id, MAX(amount) AS maxAmount, MIN(amount) AS minAmount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "MAX" in sql_upper
        assert "MIN" in sql_upper

    def test_unwind_distinct_values(self) -> None:
        """Test UNWIND with DISTINCT to get unique values."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN DISTINCT tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "DISTINCT" in sql_upper

    def test_unwind_count_distinct(self) -> None:
        """Test UNWIND followed by COUNT DISTINCT."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, COUNT(DISTINCT tag) AS uniqueTags
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COUNT" in sql_upper
        assert "DISTINCT" in sql_upper

    def test_fraud_count_risk_factors(self) -> None:
        """Test fraud scenario: count risk factors per account."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        WITH a, tag
        WHERE tag IN ['high_risk', 'suspicious', 'flagged']
        RETURN a.id, COUNT(tag) AS riskCount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COUNT" in sql_upper

    def test_fraud_sum_scores(self) -> None:
        """Test fraud scenario: sum risk scores."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.scores AS score
        RETURN a.id, SUM(score) AS totalRisk
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "SUM" in sql_upper

    def test_unwind_multiple_aggregations(self) -> None:
        """Test UNWIND with multiple aggregations."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.amounts AS amount
        RETURN a.id,
               COUNT(amount) AS cnt,
               SUM(amount) AS total,
               AVG(amount) AS avg
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COUNT" in sql_upper
        assert "SUM" in sql_upper
        assert "AVG" in sql_upper
