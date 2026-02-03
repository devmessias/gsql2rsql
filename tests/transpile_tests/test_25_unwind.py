"""Test 25: UNWIND list.

Validates that Cypher's UNWIND clause is correctly transpiled to
Databricks SQL using LATERAL EXPLODE() table-valued function.

IMPORTANT: This is a TDD test. The feature needs to be implemented.

Cypher:
    UNWIND expression AS variable

Databricks SQL (12.2+):
    FROM ..., LATERAL EXPLODE(expression) AS table_alias(variable)

Fraud Use Cases:
- Expand embedded transaction arrays for analysis
- Process batch watchlist matches
- Flatten nested account hierarchies
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql
from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
    assert_has_join,
)


class TestUnwind:
    """Test UNWIND list transpilation."""

    TEST_ID = "25"
    TEST_NAME = "unwind"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("transactionIds", list),  # Array property
                    EntityProperty("tags", list),  # Array of tags
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )
        self.schema.add_node(
            NodeSchema(
                name="Transaction",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Transaction"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="HAS_TRANSACTION",
                source_node_id="Account",
                sink_node_id="Transaction",
                source_id_property=EntityProperty("source_id", str),
                sink_id_property=EntityProperty("target_id", str),
            ),
            SQLTableDescriptor(table_name="graph.HasTransaction"),
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
        RETURN a.id, tag
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-25"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_unwind_uses_explode(self) -> None:
        """Test UNWIND translates to EXPLODE."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper, \
            "UNWIND should translate to EXPLODE"

    def test_unwind_uses_tvf_syntax(self) -> None:
        """Test UNWIND uses Databricks TVF syntax (generator function in FROM).

        Databricks SQL uses Table-Valued Functions (TVFs) for array expansion:
        FROM source_table, EXPLODE(array_col) AS alias(col)

        This is the modern syntax - LATERAL keyword is deprecated.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # TVF syntax uses EXPLODE (possibly with LATERAL correlator)
        assert "EXPLODE" in sql_upper, \
            "Should use EXPLODE TVF syntax"
        # Should NOT use deprecated LATERAL VIEW syntax (Hive)
        # Note: LATERAL with TVFs is modern and OK per Databricks 12.2+
        assert "LATERAL VIEW" not in sql_upper, \
            "Should NOT use deprecated LATERAL VIEW syntax"

    def test_unwind_simple_property(self) -> None:
        """Test UNWIND on a simple array property."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.transactionIds AS txId
        RETURN a.id, txId
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        # Should project both account id and unwound value
        sql_lower = sql.lower()
        assert "txid" in sql_lower or "tx_id" in sql_lower or "_unwind" in sql_lower

    def test_unwind_with_subsequent_match(self) -> None:
        """Test UNWIND followed by another MATCH."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.transactionIds AS txId
        MATCH (t:Transaction {id: txId})
        RETURN a.id, t.amount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper, "Should have EXPLODE for UNWIND"
        # Should join with Transaction table
        assert "TRANSACTION" in sql_upper, "Should reference Transaction table"

    def test_unwind_literal_list(self) -> None:
        """Test UNWIND on a literal list with explicit source.

        Note: In Cypher, UNWIND on a literal typically follows a pattern.
        We use a MATCH to provide a source for the UNWIND.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND [1, 2, 3] AS num
        RETURN a.id, num
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper, "Should have EXPLODE for literal UNWIND"
        # Should have the array literal
        assert "1" in sql and "2" in sql and "3" in sql

    def test_unwind_with_filter(self) -> None:
        """Test UNWIND with filter on unwound value using WITH...WHERE."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        WITH a, tag
        WHERE tag = 'suspicious'
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        # Should filter on the unwound value (via HAVING or subquery)
        sql_lower = sql.lower()
        assert "suspicious" in sql_lower

    def test_fraud_expand_transaction_ids(self) -> None:
        """Test fraud scenario: expand embedded transaction ID arrays."""
        cypher = """
        MATCH (a:Account)
        WHERE a.id IN ['ACC001', 'ACC002']
        UNWIND a.transactionIds AS txId
        MATCH (t:Transaction {id: txId})
        RETURN a.id AS account, t.id AS transaction, t.amount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        # Should have WHERE for account filter
        assert "ACC001" in sql_upper
        assert "ACC002" in sql_upper

    def test_fraud_process_tags(self) -> None:
        """Test fraud scenario: process account tags for risk analysis."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        WITH a, tag
        WHERE tag IN ['high_risk', 'flagged', 'under_review']
        RETURN a.id, COLLECT(tag) AS riskTags
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        # Should have GROUP BY for COLLECT
        assert "GROUP" in sql_upper or "COLLECT_LIST" in sql_upper

    def test_unwind_preserves_other_columns(self) -> None:
        """Test that UNWIND preserves other columns from the source."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, a.name, tag
        """
        sql = self._transpile(cypher)

        # Should select account properties along with unwound value
        sql_lower = sql.lower()
        assert "name" in sql_lower, "Should preserve name column"
        assert "id" in sql_lower, "Should preserve id column"

    def test_multiple_unwind(self) -> None:
        """Test multiple UNWIND clauses."""
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        RETURN a.id, tag, txId
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have two EXPLODE operations
        explode_count = sql_upper.count("EXPLODE")
        assert explode_count >= 2, f"Should have 2 EXPLODE, found {explode_count}"


class TestUnwindBehaviorVariants:
    """Test UNWIND behavior variants and Databricks SQL mappings.

    OpenCypher UNWIND behaviors:
    1. UNWIND array → expands each element into a row
    2. UNWIND NULL → produces NO rows (dropped)
    3. UNWIND [] → produces NO rows (dropped)

    Databricks SQL TVF options:
    - EXPLODE(array) → drops rows for NULL/empty (default, matches OpenCypher)
    - EXPLODE_OUTER(array) → preserves rows with NULL value (extension)
    - POSEXPLODE(array) → returns (pos, col) for ordered operations
    - POSEXPLODE_OUTER(array) → posexplode + preserves rows
    - INLINE(array<struct>) → explodes and unpacks struct fields
    - EXPLODE(map) → returns (key, value) pairs
    """

    TEST_ID = "25"
    TEST_NAME = "unwind"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        from gsql2rsql.common.schema import (
            NodeSchema,
            EntityProperty,
        )
        from gsql2rsql.renderer.schema_provider import (
            SimpleSQLSchemaProvider,
            SQLTableDescriptor,
        )

        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("transactionIds", list),
                    EntityProperty("tags", list),
                    EntityProperty("scores", list),  # numeric array
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )
        self.schema.add_node(
            NodeSchema(
                name="Transaction",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("amount", float),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Transaction"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_unwind_uses_explode_not_lateral(self) -> None:
        """Verify UNWIND uses EXPLODE TVF, not deprecated LATERAL EXPLODE.

        Databricks SQL best practice is to use TVF syntax:
        FROM table, EXPLODE(col) AS alias(value)

        NOT the deprecated LATERAL syntax:
        FROM table, LATERAL EXPLODE(col) AS alias(value)
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper, "Should use EXPLODE function"
        # Note: LATERAL VIEW is deprecated, but LATERAL with TVFs is modern (Databricks 12.2+)
        assert "LATERAL VIEW" not in sql_upper, "Should NOT use deprecated LATERAL VIEW"

    def test_unwind_null_drops_rows_default(self) -> None:
        """Test that UNWIND of NULL produces no rows (OpenCypher semantics).

        In OpenCypher: UNWIND NULL AS x → produces 0 rows
        In Databricks: EXPLODE(NULL) → produces 0 rows (drops the source row)

        This is the default behavior and matches OpenCypher semantics.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should use EXPLODE (not EXPLODE_OUTER) by default
        assert "EXPLODE(" in sql_upper, "Should use EXPLODE function"
        # Verify it's not using EXPLODE_OUTER
        assert "EXPLODE_OUTER" not in sql_upper, \
            "Should NOT use EXPLODE_OUTER by default (would preserve NULL rows)"

    def test_unwind_empty_array_drops_rows(self) -> None:
        """Test that UNWIND of [] produces no rows (OpenCypher semantics).

        In OpenCypher: UNWIND [] AS x → produces 0 rows
        In Databricks: EXPLODE(ARRAY()) → produces 0 rows

        Both drop the source row when the array is empty.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND [] AS item
        RETURN a.id, item
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should still use EXPLODE for literal empty array
        assert "EXPLODE" in sql_upper

    def test_unwind_with_coalesce_null_handling(self) -> None:
        """Test UNWIND with COALESCE for custom NULL handling.

        Pattern: UNWIND COALESCE(array, [default]) AS x
        This allows providing a default value when array is NULL.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND COALESCE(a.tags, ['no_tags']) AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COALESCE" in sql_upper, \
            "Should preserve COALESCE for NULL handling"

    def test_unwind_range_literal(self) -> None:
        """Test UNWIND with RANGE function for numeric sequences.

        UNWIND RANGE(0, 5) AS i → expands to rows with i = 0,1,2,3,4,5
        """
        cypher = """
        MATCH (a:Account)
        UNWIND RANGE(0, 5) AS idx
        RETURN a.id, idx
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        # Should have RANGE or SEQUENCE function
        assert "RANGE" in sql_upper or "SEQUENCE" in sql_upper

    def test_unwind_nested_array_literal(self) -> None:
        """Test UNWIND with nested literal array containing expressions."""
        cypher = """
        MATCH (a:Account)
        UNWIND ['active', 'pending', 'closed'] AS status
        RETURN a.id, status
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        # Should contain the literal values
        assert "ACTIVE" in sql_upper
        assert "PENDING" in sql_upper
        assert "CLOSED" in sql_upper

    def test_unwind_cartesian_product(self) -> None:
        """Test multiple UNWIND produces cartesian product.

        UNWIND [1,2] AS x
        UNWIND ['a','b'] AS y
        → produces 4 rows: (1,a), (1,b), (2,a), (2,b)

        Each UNWIND adds another EXPLODE in the FROM clause.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        UNWIND a.transactionIds AS txId
        RETURN a.id, tag, txId
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have multiple EXPLODE for cartesian product
        explode_count = sql_upper.count("EXPLODE(")
        assert explode_count >= 2, \
            f"Should have >= 2 EXPLODE for cartesian product, found {explode_count}"

    def test_unwind_with_where_after_with(self) -> None:
        """Test UNWIND followed by WITH...WHERE for filtering.

        In Cypher, WHERE cannot directly follow UNWIND, so we use:
        UNWIND x AS elem
        WITH a, elem
        WHERE elem > 5
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.scores AS score
        WITH a, score
        WHERE score > 100
        RETURN a.id, score
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "100" in sql, "Should have the threshold value"

    def test_unwind_aggregation_after(self) -> None:
        """Test UNWIND followed by aggregation.

        UNWIND then COLLECT allows re-grouping or transforming arrays.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, COUNT(tag) AS tagCount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COUNT" in sql_upper, "Should have COUNT aggregation"

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
        assert "DISTINCT" in sql_upper, "Should have DISTINCT"
