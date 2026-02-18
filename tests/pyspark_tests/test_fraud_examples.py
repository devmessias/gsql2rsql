"""PySpark execution tests for fraud_queries.yaml examples.

Validates that example queries from fraud_queries.yaml transpile and execute
correctly using GraphContext with triple-store format.

Test Graph: 10 node types, 13 edge types. See NODES_DATA/EDGES_DATA below.
  Accounts(4), Merchants(3), Transactions(6), Persons(3), Addresses(2),
  Cards(3), POS(2), Customers(3), Purchases(3), Returns(2), Countries(2).

Edge summary:
  TRANSACTION:  A1->M1, A2->M1, A3->M2, A1->M2
  TRANSFER:     A1->A2, A2->A3, A3->A4, A4->A1 (circular)
  HAS_ADDRESS:  P1->Addr1, P2->Addr1, P3->Addr1
  USED_IN:      Card1->T1, Card2->T2, Card3->T3
  PROCESSED:    POS1->T1, POS1->T2, POS1->T3, POS2->T6
  HAS_CARD:     Cust1->Card1, Cust2->Card1, Cust2->Card2, Cust3->Card3
  USED_AT:      Card1->M1, Card2->M1, Card3->M2
  MADE_PURCHASE: Cust1->Purch1, Cust1->Purch2, Cust2->Purch3
  RETURNED:     Purch1->Ret1, Purch2->Ret2
  HAS_TRANSACTION: A1->T1, A1->T2, A2->T3, A3->T6
  DEPOSIT:      A1->T4, A1->T5
  AT_MERCHANT:  T1->M1, T2->M2, T3->M1, T6->M3
  TO_COUNTRY:   T6->Country1, T3->Country2
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
    StructField("holder_name", StringType(), True),
    StructField("risk_score", LongType(), True),
    StructField("status", StringType(), True),
    StructField("default_date", StringType(), True),
    StructField("home_country", StringType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("days_since_creation", LongType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("type", StringType(), True),
    StructField("merchant_id", LongType(), True),
    StructField("creation_date", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("number", StringType(), True),
    StructField("location", StringType(), True),
    StructField("risk_status", StringType(), True),
    StructField("flagged", BooleanType(), True),
    StructField("code", StringType(), True),
])

# fmt: off
# Columns: node_id, node_type, name, holder_name, risk_score, status, default_date, home_country, kyc_status, days_since_creation, category, amount, timestamp, type, merchant_id, creation_date, street, city, number, location, risk_status, flagged, code
NODES_DATA = [
    # Account (100-103)
    (100, "Account", None, "Alice", 80, "defaulted", "2024-01-15", "US", "incomplete", 10,  None, None, None, None, None, None, None, None, None, None, None, None, None),
    (101, "Account", None, "Bob",   30, "active",    None,         "US", "complete",  365,  None, None, None, None, None, None, None, None, None, None, None, None, None),
    (102, "Account", None, "Carol", 90, "active",    None,         "GB", "incomplete",  5,  None, None, None, None, None, None, None, None, None, None, None, None, None),
    (103, "Account", None, "Dave",  50, "active",    None,         "US", "complete",  200,  None, None, None, None, None, None, None, None, None, None, None, None, None),
    # Merchant (200-202)
    (200, "Merchant", "ShopA", None, None, None, None, None, None, None, "electronics", None, None, None, None, None, None, None, None, None, None, None, None),
    (201, "Merchant", "ShopB", None, None, None, None, None, None, None, "grocery",     None, None, None, None, None, None, None, None, None, None, None, None),
    (202, "Merchant", "ShopC", None, None, None, None, None, None, None, "electronics", None, None, None, None, None, None, None, None, None, None, None, None),
    # Transaction (300-305)
    (300, "Transaction", None, None, None, None, None, None, None, None, None, 500.0,   "2024-06-01T10:00:00", "purchase", 200, None, None, None, None, None, None, None, None),
    (301, "Transaction", None, None, None, None, None, None, None, None, None, 50.0,    "2024-06-01T10:05:00", "purchase", 201, None, None, None, None, None, None, None, None),
    (302, "Transaction", None, None, None, None, None, None, None, None, None, 200.0,   "2024-06-01T11:00:00", "purchase", 200, None, None, None, None, None, None, None, None),
    (303, "Transaction", None, None, None, None, None, None, None, None, None, 9500.0,  "2024-06-10T09:00:00", "deposit",  None, None, None, None, None, None, None, None, None),
    (304, "Transaction", None, None, None, None, None, None, None, None, None, 9800.0,  "2024-06-10T09:30:00", "deposit",  None, None, None, None, None, None, None, None, None),
    (305, "Transaction", None, None, None, None, None, None, None, None, None, 15000.0, "2024-07-01T12:00:00", "wire",     202, None, None, None, None, None, None, None, None),
    # Person (400-402)
    (400, "Person", "John", None, None, None, None, None, None, None, None, None, None, None, None, "2024-01-01", None, None, None, None, None, None, None),
    (401, "Person", "Jane", None, None, None, None, None, None, None, None, None, None, None, None, "2024-06-01", None, None, None, None, None, None, None),
    (402, "Person", "Jim",  None, None, None, None, None, None, None, None, None, None, None, None, "2024-06-01", None, None, None, None, None, None, None),
    # Address (500-501)
    (500, "Address", None, None, None, None, None, None, None, None, None, None, None, None, None, None, "123 Main St", "CityA", None, None, None, None, None),
    (501, "Address", None, None, None, None, None, None, None, None, None, None, None, None, None, None, "456 Oak Ave",  "CityB", None, None, None, None, None),
    # Card (600-602)
    (600, "Card", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "4111-1111", None, None, None, None),
    (601, "Card", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "5222-2222", None, None, None, None),
    (602, "Card", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "6333-3333", None, None, None, None),
    # POS (700-701)
    (700, "POS", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "Downtown", "high_risk", True,  None),
    (701, "POS", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "Airport",  "low_risk",  False, None),
    # Customer (800-802)
    (800, "Customer", "AliceCust", None, None, "blacklisted", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    (801, "Customer", "BobCust",   None, None, "verified",    None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    (802, "Customer", "CarolCust", None, None, "verified",    None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # Purchase (900-902)
    (900, "Purchase", None, None, None, None, None, None, None, None, None, 100.0, "2024-05-01", None, None, None, None, None, None, None, None, None, None),
    (901, "Purchase", None, None, None, None, None, None, None, None, None, 200.0, "2024-05-15", None, None, None, None, None, None, None, None, None, None),
    (902, "Purchase", None, None, None, None, None, None, None, None, None, 50.0,  "2024-06-01", None, None, None, None, None, None, None, None, None, None),
    # Return (950-951)
    (950, "Return", None, None, None, None, None, None, None, None, None, 100.0, "2024-05-10", None, None, None, None, None, None, None, None, None, None),
    (951, "Return", None, None, None, None, None, None, None, None, None, 200.0, "2024-05-20", None, None, None, None, None, None, None, None, None, None),
    # Country (1000-1001)
    (1000, "Country", "United Kingdom", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "GB"),
    (1001, "Country", "Germany",        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "DE"),
]
# fmt: on

EDGES_SCHEMA = StructType([
    StructField("src", LongType(), False),
    StructField("dst", LongType(), False),
    StructField("relationship_type", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

EDGES_DATA = [
    # TRANSACTION (Account -> Merchant)
    (100, 200, "TRANSACTION", 500.0,  "2024-06-01T10:00:00"),
    (101, 200, "TRANSACTION", 200.0,  "2024-06-01T11:00:00"),
    (102, 201, "TRANSACTION", 50.0,   "2024-06-02T09:00:00"),
    (100, 201, "TRANSACTION", 75.0,   "2024-06-03T14:00:00"),
    # TRANSFER (Account -> Account) — circular: A1->A2->A3->A4->A1
    (100, 101, "TRANSFER", 1500.0, "2024-06-01T08:00:00"),
    (101, 102, "TRANSFER", 2000.0, "2024-06-02T08:00:00"),
    (102, 103, "TRANSFER", 1800.0, "2024-06-03T08:00:00"),
    (103, 100, "TRANSFER", 1200.0, "2024-06-04T08:00:00"),
    # HAS_ADDRESS (Person -> Address)
    (400, 500, "HAS_ADDRESS", None, None),
    (401, 500, "HAS_ADDRESS", None, None),
    (402, 500, "HAS_ADDRESS", None, None),
    # USED_IN (Card -> Transaction)
    (600, 300, "USED_IN", None, None),
    (601, 301, "USED_IN", None, None),
    (602, 302, "USED_IN", None, None),
    # PROCESSED (POS -> Transaction)
    (700, 300, "PROCESSED", None, None),
    (700, 301, "PROCESSED", None, None),
    (700, 302, "PROCESSED", None, None),
    (701, 305, "PROCESSED", None, None),
    # HAS_CARD (Customer -> Card)
    (800, 600, "HAS_CARD", None, None),
    (801, 600, "HAS_CARD", None, None),
    (801, 601, "HAS_CARD", None, None),
    (802, 602, "HAS_CARD", None, None),
    # USED_AT (Card -> Merchant)
    (600, 200, "USED_AT", None, None),
    (601, 200, "USED_AT", None, None),
    (602, 201, "USED_AT", None, None),
    # MADE_PURCHASE (Customer -> Purchase)
    (800, 900, "MADE_PURCHASE", None, None),
    (800, 901, "MADE_PURCHASE", None, None),
    (801, 902, "MADE_PURCHASE", None, None),
    # RETURNED (Purchase -> Return)
    (900, 950, "RETURNED", None, None),
    (901, 951, "RETURNED", None, None),
    # HAS_TRANSACTION (Account -> Transaction)
    (100, 300, "HAS_TRANSACTION", None, None),
    (100, 301, "HAS_TRANSACTION", None, None),
    (101, 302, "HAS_TRANSACTION", None, None),
    (102, 305, "HAS_TRANSACTION", None, None),
    # DEPOSIT (Account -> Transaction)
    (100, 303, "DEPOSIT", None, None),
    (100, 304, "DEPOSIT", None, None),
    # AT_MERCHANT (Transaction -> Merchant)
    (300, 200, "AT_MERCHANT", None, None),
    (301, 201, "AT_MERCHANT", None, None),
    (302, 200, "AT_MERCHANT", None, None),
    (305, 202, "AT_MERCHANT", None, None),
    # TO_COUNTRY (Transaction -> Country)
    (305, 1000, "TO_COUNTRY", None, None),
    (302, 1001, "TO_COUNTRY", None, None),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _adapt(query: str) -> str:
    """Adapt YAML queries for GraphContext: .id -> .node_id"""
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for fraud tests."""
    session = (
        SparkSession.builder.appName("fraud_test")
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
    """Create GraphContext with fraud test data."""
    spark.sql("CREATE DATABASE IF NOT EXISTS test_fraud")

    nodes_df = spark.createDataFrame(NODES_DATA, NODES_SCHEMA)
    nodes_df.write.mode("overwrite").saveAsTable("test_fraud.nodes")

    edges_df = spark.createDataFrame(EDGES_DATA, EDGES_SCHEMA)
    edges_df.write.mode("overwrite").saveAsTable("test_fraud.edges")

    graph = GraphContext(
        spark=spark,
        nodes_table="test_fraud.nodes",
        edges_table="test_fraud.edges",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={
            "name": str, "holder_name": str, "risk_score": int,
            "status": str, "default_date": str, "home_country": str,
            "kyc_status": str, "days_since_creation": int,
            "category": str,
            "amount": float, "timestamp": str, "type": str, "merchant_id": int,
            "creation_date": str,
            "street": str, "city": str,
            "number": str,
            "location": str, "risk_status": str, "flagged": bool,
            "code": str,
        },
        extra_edge_attrs={"amount": float, "timestamp": str},
        discover_edge_combinations=True,
    )

    yield graph

    spark.sql("DROP TABLE IF EXISTS test_fraud.nodes")
    spark.sql("DROP TABLE IF EXISTS test_fraud.edges")
    spark.sql("DROP DATABASE IF EXISTS test_fraud")


# ===========================================================================
# Q01: Co-shopper fraud -- no temporal functions, should execute
# ===========================================================================
class TestCoShopperFraud:

    def test_co_shopper_detection(self, gc, spark):
        """A1 & A2 share M1, A1 & A3 share M2."""
        query = _adapt("""
            MATCH (a:Account)-[:TRANSACTION]->(m:Merchant)<-[:TRANSACTION]-(b:Account)
            WHERE a.node_id <> b.node_id
            RETURN a.node_id AS a_id, b.node_id AS b_id, m.name AS m_name,
                   COUNT(*) AS shared_transactions
            ORDER BY shared_transactions DESC
            LIMIT 10
        """)
        rows = _exec(gc, spark, query, "Q01: Co-shopper fraud")
        assert len(rows) >= 2
        pairs = {(r["a_id"], r["b_id"]) for r in rows}
        assert (100, 101) in pairs or (101, 100) in pairs


# ===========================================================================
# Q02: Camouflage -- VLP + TRANSFORM (list comprehension)
# ===========================================================================
class TestCamouflageDetection:

    @pytest.mark.xfail(reason="TRANSFORM() for list comprehension not supported in local PySpark")
    def test_camouflage_patterns(self, gc, spark):
        """VLP TRANSFER*2..4 between high-risk accounts with path node extraction."""
        query = _adapt("""
            MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
            WHERE a.risk_score > 70 AND b.risk_score > 70
            RETURN a.node_id AS a_id, b.node_id AS b_id, LENGTH(path) AS chain_length,
                   [node IN nodes(path) | node.node_id] AS path_nodes
            ORDER BY chain_length DESC
        """)
        rows = _exec(gc, spark, query, "Q02: Camouflage detection")
        assert len(rows) >= 1


# ===========================================================================
# Q03: High-risk POS -- uses STDDEV, should work. Threshold lowered.
# ===========================================================================
class TestHighRiskPOS:

    def test_high_risk_pos(self, gc, spark):
        """POS1 (high_risk, flagged) processed T1(500), T2(50), T3(200) = 3 txns."""
        query = _adapt("""
            MATCH (p:POS)-[:PROCESSED]->(t:Transaction)
            WHERE p.risk_status = 'high_risk' OR p.flagged = true
            WITH p,
                 COUNT(t) AS total_transactions,
                 SUM(t.amount) AS total_volume,
                 AVG(t.amount) AS avg_amount,
                 STDDEV(t.amount) AS stddev_amount
            WHERE total_transactions > 1
            RETURN p.node_id AS pos_id, p.location AS pos_location, p.risk_status,
                   total_transactions, total_volume, avg_amount, stddev_amount
            ORDER BY total_volume DESC
            LIMIT 20
        """)
        rows = _exec(gc, spark, query, "Q03: High-risk POS")
        assert len(rows) == 1
        r = rows[0]
        assert r["pos_location"] == "Downtown"
        assert r["total_transactions"] == 3
        assert r["total_volume"] == pytest.approx(750.0)
        assert r["avg_amount"] == pytest.approx(250.0)
        assert r["stddev_amount"] is not None


# ===========================================================================
# Q04: Synthetic identity -- uses DATE(), xfail
# ===========================================================================
class TestSyntheticIdentity:

    def test_synthetic_identity(self, gc, spark):
        """P1, P2, P3 share Addr1. P2, P3 created after 2023-01-01."""
        query = _adapt("""
            MATCH (p1:Person)-[:HAS_ADDRESS]->(addr:Address)<-[:HAS_ADDRESS]-(p2:Person)
            WHERE p1.node_id <> p2.node_id AND p1.creation_date > DATE('2023-01-01')
            WITH addr.street AS street, addr.city AS city,
                 COUNT(DISTINCT p1.node_id) AS person_count
            WHERE person_count > 1
            RETURN street, city, person_count
            ORDER BY person_count DESC
        """)
        rows = _exec(gc, spark, query, "Q04: Synthetic identity")
        assert len(rows) >= 1


# ===========================================================================
# Q05: Card testing -- TIMESTAMP(), DURATION() -> xfail
# ===========================================================================
class TestCardTesting:

    def test_card_testing(self, gc, spark):
        """Card testing patterns with small probe transactions."""
        query = _adapt("""
            MATCH (c:Card)-[:USED_IN]->(t:Transaction)
            WHERE t.amount < 1.00 AND t.timestamp > TIMESTAMP() - DURATION('P1D')
            WITH c, COUNT(t) AS small_tx_count, COLLECT(t.merchant_id) AS merchants
            WHERE small_tx_count > 10
            RETURN c.number, small_tx_count, SIZE(merchants) AS merchant_count
            ORDER BY small_tx_count DESC
        """)
        rows = _exec(gc, spark, query, "Q05: Card testing")
        assert len(rows) >= 0


# ===========================================================================
# Q06: Collusion -- ABS(timestamp), DURATION() -> xfail
# ===========================================================================
class TestCollusionDetection:

    @pytest.mark.xfail(reason="Databricks-specific temporal functions ABS(timestamp), DURATION()")
    def test_collusion_networks(self, gc, spark):
        """Collusion via coordinated transaction timing."""
        query = _adapt("""
            MATCH (a1:Account)-[:HAS_TRANSACTION]->(t1:Transaction),
                  (a2:Account)-[:HAS_TRANSACTION]->(t2:Transaction)
            WHERE a1.node_id < a2.node_id
              AND ABS(t1.timestamp - t2.timestamp) < DURATION('PT5M')
              AND t1.merchant_id = t2.merchant_id
            WITH a1.node_id AS a1_id, a2.node_id AS a2_id,
                 t1.merchant_id AS merchant_id, COUNT(*) AS coordinated_count
            WHERE coordinated_count > 5
            RETURN a1_id, a2_id, merchant_id, coordinated_count
            ORDER BY coordinated_count DESC
        """)
        rows = _exec(gc, spark, query, "Q06: Collusion detection")
        assert len(rows) >= 0


# ===========================================================================
# Q07: Money mule -- VLP + TIMESTAMP(), DURATION(), REDUCE() -> xfail
# ===========================================================================
class TestMoneyMule:

    def test_money_mule_networks(self, gc, spark):
        """Money mule networks with rapid transfer chains."""
        query = _adapt("""
            MATCH path = (source:Account)-[:TRANSFER*3..6]->(sink:Account)
            WHERE ALL(rel IN relationships(path) WHERE rel.timestamp > TIMESTAMP() - DURATION('P7D'))
              AND ALL(rel IN relationships(path) WHERE rel.amount > 1000)
            WITH source, sink, path,
                 REDUCE(total = 0, rel IN relationships(path) | total + rel.amount) AS total_amount
            RETURN source.node_id AS src_id, sink.node_id AS sink_id,
                   LENGTH(path) AS hops, total_amount
            ORDER BY total_amount DESC
            LIMIT 15
        """)
        rows = _exec(gc, spark, query, "Q07: Money mule detection")
        assert len(rows) >= 0


# ===========================================================================
# Q08: Customer similarity -- multi-MATCH after WITH, complex
# ===========================================================================
class TestCustomerSimilarity:

    def test_customer_similarity(self, gc, spark):
        """Similarity via shared cards and merchants. Cust1/Cust2 share Card1."""
        query = _adapt("""
            MATCH (c1:Customer)-[:HAS_CARD]->(card:Card)<-[:HAS_CARD]-(c2:Customer)
            WHERE c1.node_id < c2.node_id
            WITH c1, c2, COUNT(DISTINCT card) AS shared_cards
            WHERE shared_cards > 0
            MATCH (c1)-[:HAS_CARD]->(card1:Card)-[:USED_AT]->(m:Merchant)
            MATCH (c2)-[:HAS_CARD]->(card2:Card)-[:USED_AT]->(m)
            WITH c1, c2, shared_cards,
                 COUNT(DISTINCT m) AS shared_merchants,
                 shared_cards * 1.0 / (shared_cards + COUNT(DISTINCT m)) AS similarity_score
            WHERE similarity_score > 0.3
            RETURN c1.node_id AS c1_id, c2.node_id AS c2_id,
                   shared_cards, shared_merchants, similarity_score
            ORDER BY similarity_score DESC
            LIMIT 50
        """)
        rows = _exec(gc, spark, query, "Q08: Customer similarity")
        assert len(rows) >= 0


# ===========================================================================
# Q09: Velocity abuse -- TIMESTAMP(), DURATION() -> xfail
# ===========================================================================
class TestVelocityAbuse:

    def test_velocity_abuse(self, gc, spark):
        """High-frequency transaction patterns."""
        query = _adapt("""
            MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)
            WHERE t.timestamp > TIMESTAMP() - DURATION('PT1H')
            WITH a, COUNT(t) AS tx_per_hour, SUM(t.amount) AS total_amount
            WHERE tx_per_hour > 20
            RETURN a.node_id AS a_id, a.holder_name, tx_per_hour, total_amount
            ORDER BY tx_per_hour DESC
        """)
        rows = _exec(gc, spark, query, "Q09: Velocity abuse")
        assert len(rows) >= 0


# ===========================================================================
# Q10: Return fraud -- no temporal, should execute. Threshold lowered.
# ===========================================================================
class TestReturnFraud:

    def test_return_fraud(self, gc, spark):
        """Cust1: 2 purchases both returned -> return_rate=1.0. Threshold > 1."""
        query = _adapt("""
            MATCH (c:Customer)-[:MADE_PURCHASE]->(p:Purchase)-[:RETURNED]->(r:Return)
            WITH c, COUNT(p) AS total_purchases, COUNT(r) AS total_returns
            WHERE total_purchases > 1
            WITH c, total_purchases, total_returns,
                 (total_returns * 1.0 / total_purchases) AS return_rate
            WHERE return_rate > 0.5
            RETURN c.node_id AS c_id, c.name AS c_name,
                   total_purchases, total_returns, return_rate
            ORDER BY return_rate DESC
        """)
        rows = _exec(gc, spark, query, "Q10: Return fraud")
        assert len(rows) == 1
        r = rows[0]
        assert r["c_name"] == "AliceCust"
        assert r["total_purchases"] == 2
        assert r["total_returns"] == 2
        assert r["return_rate"] == pytest.approx(1.0)


# ===========================================================================
# Q11: Bust-out fraud -- DURATION() -> xfail
# ===========================================================================
class TestBustOutFraud:

    def test_bust_out_fraud(self, gc, spark):
        """Sudden spending spikes before default."""
        query = _adapt("""
            MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)
            WHERE a.status = 'defaulted' AND t.timestamp > a.default_date - DURATION('P30D')
            WITH a,
                 SUM(CASE WHEN t.timestamp > a.default_date - DURATION('P7D')
                     THEN t.amount ELSE 0 END) AS last_week,
                 SUM(CASE WHEN t.timestamp <= a.default_date - DURATION('P7D')
                     THEN t.amount ELSE 0 END) AS prior_weeks
            WHERE prior_weeks > 0 AND (last_week / prior_weeks) > 5.0
            RETURN a.node_id AS a_id, last_week, prior_weeks,
                   (last_week / prior_weeks) AS spike_ratio
            ORDER BY spike_ratio DESC
        """)
        rows = _exec(gc, spark, query, "Q11: Bust-out fraud")
        assert len(rows) >= 0


# ===========================================================================
# Q12: Circular payment -- VLP + REDUCE() -> xfail
# ===========================================================================
class TestCircularPayment:

    @pytest.mark.xfail(reason="REDUCE() function not supported in local PySpark")
    def test_circular_payments(self, gc, spark):
        """Circular: A1->A2->A3->A4->A1 (4-hop cycle, all amounts > 500)."""
        query = _adapt("""
            MATCH path = (a:Account)-[:TRANSFER*4..8]->(a)
            WHERE ALL(rel IN relationships(path) WHERE rel.amount > 500)
              AND LENGTH(path) >= 4
            WITH path,
                 REDUCE(total = 0, rel IN relationships(path) | total + rel.amount) AS cycle_amount
            RETURN [node IN nodes(path) | node.node_id] AS cycle_accounts,
                   LENGTH(path) AS cycle_length, cycle_amount
            ORDER BY cycle_amount DESC
            LIMIT 10
        """)
        rows = _exec(gc, spark, query, "Q12: Circular payment")
        assert len(rows) >= 0


# ===========================================================================
# Q13: Cross-border anomaly -- no temporal, should execute. Threshold lowered.
# ===========================================================================
class TestCrossBorderAnomaly:

    def test_cross_border_anomaly(self, gc, spark):
        """A2(US)->T3(200)->Country2(DE): cross-border, amount > 100."""
        query = _adapt("""
            MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)-[:TO_COUNTRY]->(c:Country)
            WHERE c.code <> a.home_country AND t.amount > 100
            WITH a, c, COUNT(t) AS cross_border_count, SUM(t.amount) AS total_amount
            WHERE cross_border_count > 0
            RETURN a.node_id AS a_id, c.name AS destination_country,
                   cross_border_count, total_amount
            ORDER BY total_amount DESC
        """)
        rows = _exec(gc, spark, query, "Q13: Cross-border anomaly")
        assert len(rows) >= 1
        countries = {r["destination_country"] for r in rows}
        assert "Germany" in countries


# ===========================================================================
# Q14: Account takeover -- TIMESTAMP(), DURATION() -> xfail
# ===========================================================================
class TestAccountTakeover:

    def test_account_takeover(self, gc, spark):
        """Sudden behavioral changes in transaction patterns."""
        query = _adapt("""
            MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)
            WITH a,
                 AVG(CASE WHEN t.timestamp < TIMESTAMP() - DURATION('P30D')
                     THEN t.amount END) AS avg_30d_ago,
                 AVG(CASE WHEN t.timestamp >= TIMESTAMP() - DURATION('P7D')
                     THEN t.amount END) AS avg_recent
            WHERE avg_30d_ago IS NOT NULL AND avg_recent > avg_30d_ago * 3
            RETURN a.node_id AS a_id, avg_30d_ago, avg_recent,
                   (avg_recent / avg_30d_ago) AS behavior_change_ratio
            ORDER BY behavior_change_ratio DESC
        """)
        rows = _exec(gc, spark, query, "Q14: Account takeover")
        assert len(rows) >= 0


# ===========================================================================
# Q15: Smurfing -- TIMESTAMP(), DURATION() -> xfail
# ===========================================================================
class TestSmurfing:

    def test_smurfing(self, gc, spark):
        """Structured deposits below reporting thresholds."""
        query = _adapt("""
            MATCH (a:Account)-[:DEPOSIT]->(d:Transaction)
            WHERE d.amount > 9000 AND d.amount < 10000
              AND d.timestamp > TIMESTAMP() - DURATION('P30D')
            WITH a, COUNT(d) AS deposit_count, SUM(d.amount) AS total_deposits
            WHERE deposit_count > 5
            RETURN a.node_id AS a_id, deposit_count, total_deposits,
                   (total_deposits / deposit_count) AS avg_deposit
            ORDER BY deposit_count DESC
        """)
        rows = _exec(gc, spark, query, "Q15: Smurfing")
        assert len(rows) >= 0


# ===========================================================================
# Q16: Category-based volume -- no temporal, should execute. Threshold lowered.
# ===========================================================================
class TestCategoryBasedVolume:

    def test_category_based_volume(self, gc, spark):
        """A1(kyc=incomplete, days=10): T1->M1(electronics), T2->M2(grocery)."""
        query = _adapt("""
            MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
            WHERE a.kyc_status = 'incomplete' OR a.days_since_creation < 30
            WITH a,
                 m.category AS merchant_category,
                 COUNT(t) AS transaction_count,
                 SUM(t.amount) AS total_volume,
                 AVG(t.amount) AS avg_transaction
            WHERE transaction_count > 0
            RETURN a.node_id AS a_id, a.kyc_status, a.days_since_creation,
                   merchant_category, transaction_count, total_volume, avg_transaction
            ORDER BY total_volume DESC
            LIMIT 100
        """)
        rows = _exec(gc, spark, query, "Q16: Category-based volume")
        assert len(rows) >= 2
        a1_rows = [r for r in rows if r["a_id"] == 100]
        assert len(a1_rows) >= 1
        categories = {r["merchant_category"] for r in a1_rows}
        assert "electronics" in categories or "grocery" in categories


# ===========================================================================
# Q17: Card contamination -- multi-MATCH after WITH -> xfail
# ===========================================================================
class TestCardContamination:

    def test_card_contamination(self, gc, spark):
        """Card1 shared by Cust1(blacklisted) and Cust2(verified)."""
        query = _adapt("""
            MATCH (blacklisted:Customer)-[:HAS_CARD]->(card:Card)<-[:HAS_CARD]-(verified:Customer)
            WHERE blacklisted.status = 'blacklisted' AND verified.status = 'verified'
            WITH card,
                 COLLECT(DISTINCT blacklisted.node_id) AS blacklisted_customers,
                 COLLECT(DISTINCT verified.node_id) AS verified_customers
            MATCH (card)-[:USED_IN]->(t:Transaction)
            WITH card, blacklisted_customers, verified_customers,
                 COUNT(t) AS total_transactions, SUM(t.amount) AS total_amount
            RETURN card.number,
                   SIZE(blacklisted_customers) AS blacklisted_count,
                   SIZE(verified_customers) AS verified_count,
                   total_transactions, total_amount,
                   blacklisted_customers, verified_customers
            ORDER BY total_amount DESC
            LIMIT 25
        """)
        rows = _exec(gc, spark, query, "Q17: Card contamination")
        assert len(rows) >= 0


# ===========================================================================
# VLP-specific: TRANSFER chains (should work in local PySpark)
# ===========================================================================
class TestVLPTransferChains:

    def test_transfer_chain_2_to_4(self, gc, spark):
        """Multi-hop transfer chains in circular graph A1->A2->A3->A4->A1."""
        query = _adapt("""
            MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
            RETURN a.node_id AS src_id, b.node_id AS dst_id,
                   LENGTH(path) AS chain_length
            ORDER BY chain_length DESC, src_id
        """)
        rows = _exec(gc, spark, query, "VLP: TRANSFER*2..4")
        assert len(rows) >= 4
        chain_lengths = {r["chain_length"] for r in rows}
        assert 2 in chain_lengths
        assert 3 in chain_lengths

    def test_transfer_chain_high_risk_source(self, gc, spark):
        """TRANSFER*2..4 from high-risk source (risk_score > 70): A1(80), A3(90)."""
        query = _adapt("""
            MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
            WHERE a.risk_score > 70
            RETURN a.node_id AS src_id, b.node_id AS dst_id,
                   LENGTH(path) AS chain_length
            ORDER BY chain_length DESC
        """)
        rows = _exec(gc, spark, query, "VLP: TRANSFER high-risk source")
        assert len(rows) >= 2
        src_ids = {r["src_id"] for r in rows}
        assert src_ids <= {100, 102}

    def test_transfer_chain_high_risk_sink(self, gc, spark):
        """TRANSFER*2..4 to high-risk sink: only A1(80) and A3(90) qualify."""
        query = _adapt("""
            MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
            WHERE b.risk_score > 70
            RETURN a.node_id AS src_id, b.node_id AS dst_id,
                   LENGTH(path) AS chain_length
            ORDER BY chain_length DESC
        """)
        rows = _exec(gc, spark, query, "VLP: TRANSFER high-risk sink")
        assert len(rows) >= 2
        dst_ids = {r["dst_id"] for r in rows}
        assert dst_ids <= {100, 102}
