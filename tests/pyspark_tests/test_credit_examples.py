"""PySpark execution tests for credit_queries.yaml examples.

Validates that credit analysis queries from credit_queries.yaml transpile
and execute correctly using GraphContext with triple-store format.

Test Graph (credit domain):
    Customers: Alice(active), Bob(active), Carol(inactive), Dave(active), Eve(active)
    Accounts:  A1(Alice,10000), A2(Alice,5000), A3(Bob,25000), A4(Carol,3000),
               A5(Dave,15000), A6(Eve,8000), A7(Eve,6000)
    Transactions: T1..T12 (various types/categories across accounts)
    Loans: L1(Alice,active), L2(Bob,active), L3(Bob,active), L4(Carol,defaulted),
           L5(Dave,active), L6(Dave,active), L7(Dave,active)
    Payments: P1..P10 (on_time or late payments on loans)
    CreditCards: CC1(Alice,10000), CC2(Bob,5000), CC3(Dave,15000)

    HAS_ACCOUNT:      Alice->{A1,A2}, Bob->A3, Carol->A4, Dave->A5, Eve->{A6,A7}
    TRANSACTION:      A1->{T1,T2,T3}, A3->{T4,T5}, A4->{T6}, A5->{T7,T8}, A6->{T9,T10}, A7->{T11,T12}
    HAS_LOAN:         Alice->L1, Bob->{L2,L3}, Carol->L4, Dave->{L5,L6,L7}
    PAYMENT:          L1->{P1,P2}, L2->{P3,P4,P5,P6,P7,P8,P9}, L4->P10
    TRANSFER:         A1->A2, A3->A5, A5->A6
    HAS_CARD:         Alice->CC1, Bob->CC2, Dave->CC3
    KNOWS:            Alice->Bob, Bob->Carol, Carol->Dave, Dave->Eve
    CO_BORROWER:      Alice->L1, Bob->L1, Dave->L5, Eve->L5
    CARD_TRANSACTION: CC1->{T1}, CC2->{T4}, CC3->{T7}
"""

import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gsql2rsql.graph_context import GraphContext

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------
NODES_SCHEMA = StructType([
    StructField("node_id", LongType(), False),
    StructField("node_type", StringType(), False),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("customer_id", LongType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("origination_date", StringType(), True),
    StructField("on_time", BooleanType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("number", StringType(), True),
])

NODES_DATA = [
    # Customers (node_id 1-5)
    (1,  "Customer", "Alice", "active",   None,    None, None,    None, None, None, None,  None, None,    None, None),
    (2,  "Customer", "Bob",   "active",   None,    None, None,    None, None, None, None,  None, None,    None, None),
    (3,  "Customer", "Carol", "inactive", None,    None, None,    None, None, None, None,  None, None,    None, None),
    (4,  "Customer", "Dave",  "active",   None,    None, None,    None, None, None, None,  None, None,    None, None),
    (5,  "Customer", "Eve",   "active",   None,    None, None,    None, None, None, None,  None, None,    None, None),
    # Accounts (node_id 101-107)
    (101, "Account", None, None, 10000.0,  1,    None,    None, None, None, None,  None, None,    None, None),
    (102, "Account", None, None,  5000.0,  1,    None,    None, None, None, None,  None, None,    None, None),
    (103, "Account", None, None, 25000.0,  2,    None,    None, None, None, None,  None, None,    None, None),
    (104, "Account", None, None,  3000.0,  3,    None,    None, None, None, None,  None, None,    None, None),
    (105, "Account", None, None, 15000.0,  4,    None,    None, None, None, None,  None, None,    None, None),
    (106, "Account", None, None,  8000.0,  5,    None,    None, None, None, None,  None, None,    None, None),
    (107, "Account", None, None,  6000.0,  5,    None,    None, None, None, None,  None, None,    None, None),
    # Transactions (node_id 201-212)
    (201, "Transaction", None, None, None, None, 500.0,  "2024-01-15", "purchase",  "retail",       None, None, None, None, None),
    (202, "Transaction", None, None, None, None, 200.0,  "2024-02-10", "overdraft", "fee",          None, None, None, None, None),
    (203, "Transaction", None, None, None, None, 3000.0, "2024-03-01", "purchase",  "income",       None, None, None, None, None),
    (204, "Transaction", None, None, None, None, 1500.0, "2024-01-20", "purchase",  "income",       None, None, None, None, None),
    (205, "Transaction", None, None, None, None, 100.0,  "2024-02-05", "NSF",       "fee",          None, None, None, None, None),
    (206, "Transaction", None, None, None, None, 50.0,   "2024-03-15", "late_fee",  "fee",          None, None, None, None, None),
    (207, "Transaction", None, None, None, None, 2000.0, "2024-01-10", "purchase",  "income",       None, None, None, None, None),
    (208, "Transaction", None, None, None, None, 800.0,  "2024-02-20", "purchase",  "debt_payment", None, None, None, None, None),
    (209, "Transaction", None, None, None, None, 750.0,  "2024-01-25", "purchase",  "retail",       None, None, None, None, None),
    (210, "Transaction", None, None, None, None, 1200.0, "2024-02-28", "purchase",  "income",       None, None, None, None, None),
    (211, "Transaction", None, None, None, None, 400.0,  "2024-03-10", "purchase",  "debt_payment", None, None, None, None, None),
    (212, "Transaction", None, None, None, None, 600.0,  "2024-03-20", "purchase",  "retail",       None, None, None, None, None),
    # Loans (node_id 301-307)  -- status + balance + amount + interest_rate + origination_date
    (301, "Loan", None, "active",    45000.0, None, 50000.0,  None, None, None, 5.5,  "2020-01-15", None, None, None),
    (302, "Loan", None, "active",    28000.0, None, 30000.0,  None, None, None, 6.0,  "2021-06-01", None, None, None),
    (303, "Loan", None, "active",    18000.0, None, 20000.0,  None, None, None, 7.5,  "2019-03-20", None, None, None),
    (304, "Loan", None, "defaulted", 15000.0, None, 15000.0,  None, None, None, 8.0,  "2018-09-10", None, None, None),
    (305, "Loan", None, "active",    35000.0, None, 40000.0,  None, None, None, 9.0,  "2017-01-01", None, None, None),
    (306, "Loan", None, "active",    22000.0, None, 25000.0,  None, None, None, 7.0,  "2020-05-15", None, None, None),
    (307, "Loan", None, "active",     9000.0, None, 10000.0,  None, None, None, 8.5,  "2016-12-01", None, None, None),
    # Payments (node_id 401-410)
    (401, "Payment", None, None, None, None, 1000.0, "2024-01-01", None, None, None, None, True,  None, None),
    (402, "Payment", None, None, None, None, 1000.0, "2024-02-01", None, None, None, None, True,  None, None),
    (403, "Payment", None, None, None, None,  800.0, "2024-01-01", None, None, None, None, True,  None, None),
    (404, "Payment", None, None, None, None,  800.0, "2024-02-01", None, None, None, None, True,  None, None),
    (405, "Payment", None, None, None, None,  800.0, "2024-03-01", None, None, None, None, True,  None, None),
    (406, "Payment", None, None, None, None,  800.0, "2024-04-01", None, None, None, None, True,  None, None),
    (407, "Payment", None, None, None, None,  800.0, "2024-05-01", None, None, None, None, True,  None, None),
    (408, "Payment", None, None, None, None,  800.0, "2024-06-01", None, None, None, None, False, None, None),
    (409, "Payment", None, None, None, None,  800.0, "2024-07-01", None, None, None, None, True,  None, None),
    (410, "Payment", None, None, None, None,  500.0, "2024-01-15", None, None, None, None, False, None, None),
    # CreditCards (node_id 501-503)
    (501, "CreditCard", None, None, None, None, None, None, None, None, None, None, None, 10000.0, "4111-1111-1111-1111"),
    (502, "CreditCard", None, None, None, None, None, None, None, None, None, None, None,  5000.0, "4222-2222-2222-2222"),
    (503, "CreditCard", None, None, None, None, None, None, None, None, None, None, None, 15000.0, "4333-3333-3333-3333"),
]


EDGES_SCHEMA = StructType([
    StructField("src", LongType(), False),
    StructField("dst", LongType(), False),
    StructField("relationship_type", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

EDGES_DATA = [
    # HAS_ACCOUNT: Customer -> Account
    (1, 101, "HAS_ACCOUNT", None, None),
    (1, 102, "HAS_ACCOUNT", None, None),
    (2, 103, "HAS_ACCOUNT", None, None),
    (3, 104, "HAS_ACCOUNT", None, None),
    (4, 105, "HAS_ACCOUNT", None, None),
    (5, 106, "HAS_ACCOUNT", None, None),
    (5, 107, "HAS_ACCOUNT", None, None),
    # TRANSACTION: Account -> Transaction
    (101, 201, "TRANSACTION", None, None),
    (101, 202, "TRANSACTION", None, None),
    (101, 203, "TRANSACTION", None, None),
    (103, 204, "TRANSACTION", None, None),
    (103, 205, "TRANSACTION", None, None),
    (104, 206, "TRANSACTION", None, None),
    (105, 207, "TRANSACTION", None, None),
    (105, 208, "TRANSACTION", None, None),
    (106, 209, "TRANSACTION", None, None),
    (106, 210, "TRANSACTION", None, None),
    (107, 211, "TRANSACTION", None, None),
    (107, 212, "TRANSACTION", None, None),
    # HAS_LOAN: Customer -> Loan
    (1, 301, "HAS_LOAN", None, None),
    (2, 302, "HAS_LOAN", None, None),
    (2, 303, "HAS_LOAN", None, None),
    (3, 304, "HAS_LOAN", None, None),
    (4, 305, "HAS_LOAN", None, None),
    (4, 306, "HAS_LOAN", None, None),
    (4, 307, "HAS_LOAN", None, None),
    # PAYMENT: Loan -> Payment
    (301, 401, "PAYMENT", None, None),
    (301, 402, "PAYMENT", None, None),
    (302, 403, "PAYMENT", None, None),
    (302, 404, "PAYMENT", None, None),
    (302, 405, "PAYMENT", None, None),
    (302, 406, "PAYMENT", None, None),
    (302, 407, "PAYMENT", None, None),
    (302, 408, "PAYMENT", None, None),
    (302, 409, "PAYMENT", None, None),
    (304, 410, "PAYMENT", None, None),
    # TRANSFER: Account -> Account
    (101, 102, "TRANSFER", 2000.0, "2024-02-01"),
    (103, 105, "TRANSFER", 5000.0, "2024-01-15"),
    (105, 106, "TRANSFER", 3000.0, "2024-03-01"),
    # HAS_CARD: Customer -> CreditCard
    (1, 501, "HAS_CARD", None, None),
    (2, 502, "HAS_CARD", None, None),
    (4, 503, "HAS_CARD", None, None),
    # KNOWS: Customer -> Customer
    (1, 2, "KNOWS", None, None),
    (2, 3, "KNOWS", None, None),
    (3, 4, "KNOWS", None, None),
    (4, 5, "KNOWS", None, None),
    # CO_BORROWER: Customer -> Loan
    (1, 301, "CO_BORROWER", None, None),
    (2, 301, "CO_BORROWER", None, None),
    (4, 305, "CO_BORROWER", None, None),
    (5, 305, "CO_BORROWER", None, None),
    # CARD_TRANSACTION: CreditCard -> Transaction
    (501, 201, "CARD_TRANSACTION", None, None),
    (502, 204, "CARD_TRANSACTION", None, None),
    (503, 207, "CARD_TRANSACTION", None, None),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _adapt(query: str) -> str:
    """Adapt YAML queries for GraphContext: .id -> .node_id."""
    return re.sub(r"\.id\b", ".node_id", query)


def _exec(gc, spark, query, desc=""):
    """Transpile, execute, and return collected rows."""
    sql = gc.transpile(query)
    print(f"\n=== {desc} ===\n{sql}")
    rows = spark.sql(sql).collect()
    print(f"Result: {len(rows)} rows")
    for i, r in enumerate(rows[:10]):
        print(f"  [{i}] {r.asDict()}")
    return rows


def _transpile_only(gc, query, desc=""):
    """Transpile without executing -- verify no crash during transpilation."""
    sql = gc.transpile(query)
    print(f"\n=== {desc} (transpile only) ===\n{sql}")
    return sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for credit tests."""
    session = (
        SparkSession.builder.appName("credit_test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def gc(spark):
    """Create GraphContext with credit test data."""
    spark.sql("CREATE DATABASE IF NOT EXISTS test_credit")

    nodes_df = spark.createDataFrame(NODES_DATA, NODES_SCHEMA)
    nodes_df.write.mode("overwrite").saveAsTable("test_credit.nodes")

    edges_df = spark.createDataFrame(EDGES_DATA, EDGES_SCHEMA)
    edges_df.write.mode("overwrite").saveAsTable("test_credit.edges")

    graph = GraphContext(
        spark=spark,
        nodes_table="test_credit.nodes",
        edges_table="test_credit.edges",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={
            "name": str, "status": str, "balance": float,
            "customer_id": int, "amount": float, "timestamp": str,
            "type": str, "category": str, "interest_rate": float,
            "origination_date": str, "on_time": bool,
            "credit_limit": float, "number": str,
        },
        extra_edge_attrs={
            "amount": float, "timestamp": str,
        },
        discover_edge_combinations=True,
    )

    yield graph

    spark.sql("DROP TABLE IF EXISTS test_credit.nodes")
    spark.sql("DROP TABLE IF EXISTS test_credit.edges")
    spark.sql("DROP DATABASE IF EXISTS test_credit")


# ===========================================================================
# Queries that CAN execute (no Databricks-specific temporal functions)
# ===========================================================================
class TestPaymentReliability:
    """Q02: Payment consistency assessment."""

    def test_q02_original_thresholds(self, gc, spark):
        """Bob has 7 payments on L2 (6/7 on_time=0.857), fails > 0.95 threshold."""
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)-[:PAYMENT]->(p:Payment)
            WHERE l.status = 'active'
            WITH c, l,
                 COUNT(p) AS total_payments,
                 SUM(CASE WHEN p.on_time = true THEN 1 ELSE 0 END) AS on_time_payments
            WHERE total_payments > 6
            WITH c, l, total_payments, on_time_payments,
                 (on_time_payments * 1.0 / total_payments) AS on_time_rate
            WHERE on_time_rate > 0.95
            RETURN c.node_id AS cust_id, c.name AS cust_name, l.amount AS loan_amount,
                   on_time_rate, total_payments
            ORDER BY loan_amount DESC
        """)
        rows = _exec(gc, spark, query, "Q02: Payment reliability")
        assert len(rows) == 0

    def test_q02_relaxed_thresholds(self, gc, spark):
        """Relaxed: total_payments > 5 AND on_time_rate > 0.8 -> Bob qualifies."""
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)-[:PAYMENT]->(p:Payment)
            WHERE l.status = 'active'
            WITH c, l,
                 COUNT(p) AS total_payments,
                 SUM(CASE WHEN p.on_time = true THEN 1 ELSE 0 END) AS on_time_payments
            WHERE total_payments > 5
            WITH c, l, total_payments, on_time_payments,
                 (on_time_payments * 1.0 / total_payments) AS on_time_rate
            WHERE on_time_rate > 0.8
            RETURN c.node_id AS cust_id, c.name AS cust_name, l.amount AS loan_amount,
                   on_time_rate, total_payments
            ORDER BY loan_amount DESC
        """)
        rows = _exec(gc, spark, query, "Q02: Payment reliability (relaxed)")
        assert len(rows) == 1
        assert rows[0]["cust_name"] == "Bob"
        assert rows[0]["total_payments"] == 7
        assert float(rows[0]["on_time_rate"]) == pytest.approx(6.0 / 7.0, abs=0.01)


class TestDebtConsolidation:
    """Q03: Only Dave has >= 3 active loans (L5,L6,L7), total_debt=66000."""

    def test_q03_debt_consolidation(self, gc, spark):
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)
            WHERE l.status = 'active'
            WITH c, COUNT(l) AS active_loans, SUM(l.balance) AS total_debt,
                 AVG(l.interest_rate) AS avg_rate
            WHERE active_loans >= 3 AND total_debt > 10000
            RETURN c.node_id AS cust_id, c.name AS cust_name,
                   active_loans, total_debt, avg_rate
            ORDER BY total_debt DESC
        """)
        rows = _exec(gc, spark, query, "Q03: Debt consolidation")
        assert len(rows) == 1
        assert rows[0]["cust_name"] == "Dave"
        assert rows[0]["active_loans"] == 3
        assert rows[0]["total_debt"] == pytest.approx(66000.0, abs=1.0)
        assert rows[0]["avg_rate"] == pytest.approx(8.167, abs=0.01)


class TestNetworkScoring:
    """Q08: Undirected KNOWS*1..2 VLP to find peers with defaulted loans.

    Carol has L4(defaulted). Bob and Dave are 1-hop from Carol via KNOWS.
    """

    def test_q08_network_scoring(self, gc, spark):
        query = _adapt("""
            MATCH (c:Customer)-[:KNOWS*1..2]-(peer:Customer)-[:HAS_LOAN]->(l:Loan)
            WHERE l.status = 'defaulted'
            WITH c, COUNT(DISTINCT peer) AS defaulted_peers,
                 COUNT(DISTINCT l) AS defaulted_loans
            WHERE defaulted_peers > 0
            RETURN c.node_id AS cust_id, c.name AS cust_name,
                   defaulted_peers, defaulted_loans,
                   (defaulted_peers * 1.0) AS network_risk_score
            ORDER BY network_risk_score DESC
        """)
        rows = _exec(gc, spark, query, "Q08: Network scoring")
        assert len(rows) > 0
        names = {r["cust_name"] for r in rows}
        assert "Bob" in names or "Dave" in names


# ===========================================================================
# Query 10: Cross-sell targeting (EXISTS pattern, no temporal -- can try)
# ===========================================================================
class TestCrossSellTargeting:

    @pytest.mark.xfail(
        reason="Transpiler bug: EXISTS subquery uses _gsql2rsql_c_id "
        "instead of _gsql2rsql_c_node_id in triple-store mode"
    )
    def test_q11_cross_sell_targeting(self, gc, spark):
        """Find cross-sell opportunities for additional credit products.

        Pattern: customers WITHOUT loans but WITH high-balance accounts.
        NOT (c)-[:HAS_LOAN]->(:Loan) AND a.balance > 5000

        Customers with loans: Alice(L1), Bob(L2,L3), Carol(L4), Dave(L5,L6,L7)
        Customers WITHOUT loans: Eve
        Eve accounts: A6(8000), A7(6000) -- both > 5000
        account_count >= 2: Eve qualifies (2 accounts)

        Expected: Eve with avg_balance=(8000+6000)/2=7000, account_count=2
        """
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_ACCOUNT]->(a:Account)
            WHERE NOT (c)-[:HAS_LOAN]->(:Loan) AND a.balance > 5000
            WITH c, AVG(a.balance) AS avg_balance, COUNT(a) AS account_count
            WHERE account_count >= 2
            RETURN c.node_id AS cust_id, c.name AS cust_name,
                   avg_balance, account_count
            ORDER BY avg_balance DESC
            LIMIT 50
        """)
        rows = _exec(gc, spark, query, "Q10: Cross-sell targeting")
        assert len(rows) == 1
        assert rows[0]["cust_name"] == "Eve"
        assert rows[0]["account_count"] == 2
        assert rows[0]["avg_balance"] == pytest.approx(7000.0, abs=1.0)


# ===========================================================================
# Query 11: Payment velocity (TIMESTAMP/DURATION -- xfail)
# ===========================================================================
class TestPaymentVelocity:

    def test_q12_payment_velocity(self, gc, spark):
        """Analyze payment velocity to detect cash flow improvements."""
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)-[:PAYMENT]->(p:Payment)
            WHERE p.timestamp > TIMESTAMP() - DURATION('P180D')
            WITH c, l,
                 AVG(CASE WHEN p.timestamp > TIMESTAMP() - DURATION('P30D')
                     THEN p.amount END) AS recent_avg,
                 AVG(CASE WHEN p.timestamp <= TIMESTAMP() - DURATION('P90D')
                     THEN p.amount END) AS historical_avg
            WHERE historical_avg > 0 AND recent_avg > historical_avg * 1.2
            RETURN c.node_id AS cust_id, c.name AS cust_name,
                   l.node_id AS loan_id, historical_avg, recent_avg,
                   ((recent_avg - historical_avg) / historical_avg) AS payment_increase_pct
            ORDER BY payment_increase_pct DESC
        """)
        _exec(gc, spark, query, "Q11: Payment velocity")


# ===========================================================================
# Query 12: Risk mitigation (TIMESTAMP/DURATION -- xfail)
# ===========================================================================
class TestRiskMitigation:

    def test_q13_risk_mitigation(self, gc, spark):
        """Identify customers suitable for credit line decreases."""
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_CARD]->(card:CreditCard)-[:CARD_TRANSACTION]->(t:Transaction)
            WHERE t.timestamp > TIMESTAMP() - DURATION('P180D')
            WITH c, card,
                 MAX(card.credit_limit) AS credit_limit,
                 MAX(t.amount) AS max_transaction,
                 AVG(t.amount) AS avg_transaction
            WHERE max_transaction < credit_limit * 0.3
              AND avg_transaction < credit_limit * 0.1
            RETURN c.node_id AS cust_id, card.node_id AS card_id,
                   credit_limit, max_transaction, avg_transaction,
                   (credit_limit - max_transaction * 3) AS suggested_new_limit
            ORDER BY suggested_new_limit DESC
        """)
        _exec(gc, spark, query, "Q12: Risk mitigation")


# ===========================================================================
# Query 13: Refinancing (TIMESTAMP/DURATION -- xfail)
# ===========================================================================
class TestRefinancing:

    def test_q14_refinancing(self, gc, spark):
        """Detect refinancing opportunities via interest rate comparison."""
        query = _adapt("""
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)
            WHERE l.status = 'active'
              AND l.origination_date < TIMESTAMP() - DURATION('P730D')
              AND l.interest_rate > 7.0
            WITH c, l, l.interest_rate AS current_rate, 5.5 AS market_rate
            WHERE current_rate > market_rate + 1.0
            RETURN c.node_id AS cust_id, c.name AS cust_name,
                   l.node_id AS loan_id, l.balance AS loan_balance,
                   current_rate, market_rate,
                   (l.balance * (current_rate - market_rate) / 100) AS annual_savings
            ORDER BY annual_savings DESC
        """)
        _exec(gc, spark, query, "Q13: Refinancing")


# ===========================================================================
# Query 14: Co-borrower analysis (no temporal -- can execute)
# ===========================================================================
class TestCoBorrowerAnalysis:

    def test_q15_co_borrower_analysis(self, gc, spark):
        """Analyze co-borrower relationships for joint credit assessment.

        CO_BORROWER pairs (c1.id < c2.id):
        - L1(301): Alice(1) & Bob(2) -> pair (Alice, Bob) on L1
        - L5(305): Dave(4) & Eve(5) -> pair (Dave, Eve) on L5

        Accounts:
        - Alice: A1(10000), A2(5000) -> avg=7500
        - Bob: A3(25000) -> avg=25000
        - Dave: A5(15000) -> avg=15000
        - Eve: A6(8000), A7(6000) -> avg=7000

        Expected 2 rows:
        - (Alice, Bob, L1): c1_avg=7500, c2_avg=25000, combined=32500
        - (Dave, Eve, L5): c1_avg=15000, c2_avg=7000, combined=22000
        """
        query = _adapt("""
            MATCH (c1:Customer)-[:CO_BORROWER]->(l:Loan)<-[:CO_BORROWER]-(c2:Customer)
            WHERE c1.node_id < c2.node_id
            MATCH (c1)-[:HAS_ACCOUNT]->(a1:Account), (c2)-[:HAS_ACCOUNT]->(a2:Account)
            WITH c1, c2, l,
                 AVG(a1.balance) AS c1_avg_balance,
                 AVG(a2.balance) AS c2_avg_balance
            RETURN c1.node_id AS c1_id, c2.node_id AS c2_id,
                   l.node_id AS loan_id, l.balance AS loan_balance,
                   c1_avg_balance, c2_avg_balance,
                   (c1_avg_balance + c2_avg_balance) AS combined_liquidity
            ORDER BY combined_liquidity DESC
        """)
        rows = _exec(gc, spark, query, "Q14: Co-borrower analysis")
        assert len(rows) == 2
        # Verify the two co-borrower pairs
        pairs = {(r["c1_id"], r["c2_id"]) for r in rows}
        assert (1, 2) in pairs  # Alice & Bob
        assert (4, 5) in pairs  # Dave & Eve

        # Check combined liquidity ordering (DESC)
        assert rows[0]["combined_liquidity"] > rows[1]["combined_liquidity"]


# ===========================================================================
# Transpilation-only smoke tests for temporal queries
# ===========================================================================
class TestTranspilationSmoke:
    """Verify that temporal-dependent queries transpile without errors.

    These queries cannot execute on local PySpark (no TIMESTAMP/DURATION),
    but transpilation itself must succeed.
    """

    @pytest.mark.parametrize("desc,query", [
        ("Q05: Liquidity VLP", """
            MATCH path = (source:Account)-[:TRANSFER*1..3]->(sink:Account)
            WHERE source.customer_id = sink.customer_id
              AND ALL(rel IN relationships(path) WHERE rel.timestamp > TIMESTAMP() - DURATION('P30D'))
            WITH source.customer_id AS cid,
                 COUNT(DISTINCT path) AS chains,
                 AVG(LENGTH(path)) AS avg_len
            RETURN cid, chains, avg_len
            ORDER BY chains DESC LIMIT 20
        """),
        ("Q06: Segmentation", """
            MATCH (c:Customer)-[:HAS_ACCOUNT]->(a:Account)-[:TRANSACTION]->(t:Transaction)
            WHERE t.timestamp > TIMESTAMP() - DURATION('P180D')
            WITH c, SUM(t.amount) AS vol, AVG(a.balance) AS bal, COUNT(DISTINCT a) AS accts
            WHERE vol > 100000 AND bal > 10000
            RETURN c.node_id AS cid, c.name AS cname, vol, bal, accts
            ORDER BY vol DESC
        """),
        ("Q07: Early warning", """
            MATCH (c:Customer)-[:HAS_ACCOUNT]->(a:Account)-[:TRANSACTION]->(t:Transaction)
            WITH c, a,
                 AVG(CASE WHEN t.timestamp > TIMESTAMP() - DURATION('P7D')
                     THEN a.balance END) AS recent,
                 AVG(CASE WHEN t.timestamp <= TIMESTAMP() - DURATION('P30D')
                       AND t.timestamp > TIMESTAMP() - DURATION('P60D')
                     THEN a.balance END) AS hist
            WHERE hist > 0 AND recent < hist * 0.5
            RETURN c.node_id AS cid, c.name AS cname, hist, recent
            ORDER BY (hist - recent) / hist DESC
        """),
        ("Q09: DTI", """
            MATCH (c:Customer)-[:HAS_ACCOUNT]->(a:Account)-[:TRANSACTION]->(t:Transaction)
            WHERE t.timestamp > TIMESTAMP() - DURATION('P90D')
            WITH c,
                 SUM(CASE WHEN t.category = 'income' THEN t.amount ELSE 0 END) AS income,
                 SUM(CASE WHEN t.category = 'debt_payment' THEN t.amount ELSE 0 END) AS debt
            WHERE income > 0
            RETURN c.node_id AS cid, c.name AS cname, income, debt,
                   (debt * 1.0 / income) AS dti
            ORDER BY dti DESC
        """),
        ("Q11: Payment velocity", """
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)-[:PAYMENT]->(p:Payment)
            WHERE p.timestamp > TIMESTAMP() - DURATION('P180D')
            WITH c, l,
                 AVG(CASE WHEN p.timestamp > TIMESTAMP() - DURATION('P30D')
                     THEN p.amount END) AS recent,
                 AVG(CASE WHEN p.timestamp <= TIMESTAMP() - DURATION('P90D')
                     THEN p.amount END) AS hist
            WHERE hist > 0 AND recent > hist * 1.2
            RETURN c.node_id AS cid, c.name AS cname, l.node_id AS lid, hist, recent
            ORDER BY (recent - hist) / hist DESC
        """),
        ("Q12: Risk mitigation", """
            MATCH (c:Customer)-[:HAS_CARD]->(card:CreditCard)-[:CARD_TRANSACTION]->(t:Transaction)
            WHERE t.timestamp > TIMESTAMP() - DURATION('P180D')
            WITH c, card,
                 MAX(card.credit_limit) AS lim,
                 MAX(t.amount) AS mx, AVG(t.amount) AS av
            WHERE mx < lim * 0.3 AND av < lim * 0.1
            RETURN c.node_id AS cid, card.node_id AS crd, lim, mx, av
            ORDER BY lim DESC
        """),
        ("Q13: Refinancing", """
            MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan)
            WHERE l.status = 'active'
              AND l.origination_date < TIMESTAMP() - DURATION('P730D')
              AND l.interest_rate > 7.0
            WITH c, l, l.interest_rate AS rate, 5.5 AS mkt
            WHERE rate > mkt + 1.0
            RETURN c.node_id AS cid, c.name AS cname,
                   l.node_id AS lid, l.balance AS bal, rate, mkt
            ORDER BY l.balance * (rate - mkt) / 100 DESC
        """),
    ])
    def test_transpile_temporal_queries(self, gc, desc, query):
        """Parametrized transpilation smoke test for temporal queries."""
        sql = _transpile_only(gc, _adapt(query), desc)
        assert "SELECT" in sql
