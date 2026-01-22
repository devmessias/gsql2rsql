"""Test 34: Date/Time functions.

Validates Date/Time function transpilation from Cypher to Databricks SQL.

Cypher date/time functions map to Databricks SQL:
- date() -> CURRENT_DATE()
- datetime() -> CURRENT_TIMESTAMP()
- date({year, month, day}) -> MAKE_DATE(year, month, day)
- datetime({...}) -> MAKE_TIMESTAMP(...)
- duration({days, hours, ...}) -> INTERVAL expressions
- duration.between(d1, d2) -> DATEDIFF(d2, d1)
- date.year, date.month, date.day -> YEAR(), MONTH(), DAY()

Fraud Use Cases:
- Filter transactions by date ranges
- Calculate time between transactions
- Extract date components for grouping
- Create date-based windows for analysis
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


class TestDateTime:
    """Test date/time function transpilation."""

    TEST_ID = "34"
    TEST_NAME = "datetime"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Transaction",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                    EntityProperty("date", str),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Transaction"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_date_current(self) -> None:
        """Test date() for current date."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, date() AS today
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "CURRENT_DATE()" in sql_upper

    def test_datetime_current(self) -> None:
        """Test datetime() for current timestamp."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, datetime() AS now
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "CURRENT_TIMESTAMP()" in sql_upper

    def test_date_from_components(self) -> None:
        """Test date({year, month, day}) construction."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, date({year: 2024, month: 1, day: 15}) AS specificDate
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MAKE_DATE" in sql_upper
        assert "2024" in sql
        assert "1" in sql
        assert "15" in sql

    def test_datetime_from_components(self) -> None:
        """Test datetime({...}) construction."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, datetime({year: 2024, month: 6, day: 15, hour: 10}) AS dt
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MAKE_TIMESTAMP" in sql_upper
        assert "2024" in sql
        assert "10" in sql

    def test_year_extraction(self) -> None:
        """Test year() function for date component extraction."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, year(t.timestamp) AS txYear
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "YEAR(" in sql_upper

    def test_month_extraction(self) -> None:
        """Test month() function."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, month(t.timestamp) AS txMonth
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MONTH(" in sql_upper

    def test_day_extraction(self) -> None:
        """Test day() function."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, day(t.timestamp) AS txDay
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "DAY(" in sql_upper

    def test_fraud_date_filter(self) -> None:
        """Test fraud scenario: filter by date range."""
        cypher = """
        MATCH (t:Transaction)
        WHERE year(t.timestamp) = 2024
        RETURN t.id, t.amount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "YEAR(" in sql_upper
        assert "2024" in sql

    def test_fraud_group_by_month(self) -> None:
        """Test fraud scenario: group transactions by month."""
        cypher = """
        MATCH (t:Transaction)
        RETURN month(t.timestamp) AS txMonth, COUNT(*) AS count
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MONTH(" in sql_upper
        assert "COUNT(" in sql_upper

    def test_hour_extraction(self) -> None:
        """Test hour() function for time component."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, hour(t.timestamp) AS txHour
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "HOUR(" in sql_upper

    def test_minute_extraction(self) -> None:
        """Test minute() function."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, minute(t.timestamp) AS txMinute
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "MINUTE(" in sql_upper

    def test_quarter_extraction(self) -> None:
        """Test quarter() function."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, quarter(t.timestamp) AS txQuarter
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "QUARTER(" in sql_upper

    def test_dayofweek_extraction(self) -> None:
        """Test dayOfWeek() function."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, dayOfWeek(t.timestamp) AS dow
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "DAYOFWEEK(" in sql_upper

    def test_week_extraction(self) -> None:
        """Test week() function."""
        cypher = """
        MATCH (t:Transaction)
        RETURN t.id, week(t.timestamp) AS txWeek
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "WEEKOFYEAR(" in sql_upper

    def test_fraud_hourly_pattern(self) -> None:
        """Test fraud scenario: detect hourly transaction patterns."""
        cypher = """
        MATCH (t:Transaction)
        RETURN hour(t.timestamp) AS txHour, SUM(t.amount) AS total
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "HOUR(" in sql_upper
        assert "SUM(" in sql_upper
