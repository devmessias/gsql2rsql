#!/usr/bin/env python3
"""Test PySpark SQL scripting support for procedural BFS.

Tests incremental SQL scripting features to identify exactly where
PySpark 4.2+ fails, then tests the full procedural BFS output.
"""

from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SQL_Scripting_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )

    print(f"PySpark version: {spark.version}")
    print(f"scripting.enabled: {spark.conf.get('spark.sql.scripting.enabled')}")

    # --- Create test data ---
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person', 25),
            ('Bob',   'Person', 30),
            ('Carol', 'Person', 35),
            ('Dave',  'Person', 28),
            ('Eve',   'Person', 22)
        AS t(node_id, node_type, age)
    """)

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS', 100),
            ('Bob',   'Carol', 'KNOWS', 200),
            ('Alice', 'Dave',  'KNOWS', 150),
            ('Dave',  'Eve',   'KNOWS', 50),
            ('Eve',   'Bob',   'KNOWS', 75)
        AS t(src, dst, relationship_type, amount)
    """)

    # --- Test 1: Basic BEGIN...END ---
    print("\n=== Test 1: Basic BEGIN...END ===")
    try:
        spark.sql("BEGIN DECLARE x INT DEFAULT 1; END")
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 2: WHILE loop ---
    print("\n=== Test 2: WHILE loop ===")
    try:
        spark.sql("""
        BEGIN
          DECLARE i INT DEFAULT 0;
          WHILE i < 3 DO
            SET i = i + 1;
          END WHILE;
        END
        """)
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 3: EXECUTE IMMEDIATE (CREATE VIEW) ---
    print("\n=== Test 3: EXECUTE IMMEDIATE (CREATE VIEW) ===")
    try:
        spark.sql("""
        BEGIN
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW test_exec AS SELECT 1 AS x';
        END
        """)
        r = spark.sql("SELECT * FROM test_exec").collect()
        print(f"  OK: {r}")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 4: SET variable via EXECUTE IMMEDIATE ---
    print("\n=== Test 4: SET variable via EXECUTE IMMEDIATE ===")
    try:
        spark.sql("""
        BEGIN
          DECLARE cnt BIGINT DEFAULT 0;
          EXECUTE IMMEDIATE
            'SET cnt = (SELECT COUNT(1) FROM nodes)';
        END
        """)
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 4b: SET variable directly (no EXECUTE IMMEDIATE) ---
    print("\n=== Test 4b: SET variable directly ===")
    try:
        spark.sql("""
        BEGIN
          DECLARE cnt BIGINT DEFAULT 0;
          SET cnt = (SELECT COUNT(1) FROM nodes);
        END
        """)
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 5: String concatenation + EXECUTE IMMEDIATE ---
    print("\n=== Test 5: String concat + EXECUTE IMMEDIATE ===")
    try:
        spark.sql("""
        BEGIN
          DECLARE depth INT DEFAULT 1;
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW test_concat_' || CAST(depth AS STRING) || ' AS SELECT 42 AS val';
        END
        """)
        r = spark.sql("SELECT * FROM test_concat_1").collect()
        print(f"  OK: {r}")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 6: IF / ELSE ---
    print("\n=== Test 6: IF / ELSE ===")
    try:
        spark.sql("""
        BEGIN
          DECLARE s STRING DEFAULT '';
          IF s = '' THEN
            SET s = 'hello';
          ELSE
            SET s = s || ' world';
          END IF;
        END
        """)
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")

    # --- Test 7: Full procedural BFS via GraphContext ---
    print("\n=== Test 7: Full procedural BFS (numbered_views) ===")
    try:
        from gsql2rsql import GraphContext

        graph = GraphContext(
            spark=spark,
            nodes_table="nodes",
            edges_table="edges",
            node_id_col="node_id",
            node_type_col="node_type",
            edge_type_col="relationship_type",
            edge_src_col="src",
            edge_dst_col="dst",
            extra_node_attrs={"age": int},
            extra_edge_attrs={"amount": int},
        )

        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """

        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n--- Generated SQL (first 500 chars) ---")
        print(sql[:500])
        print(f"--- ... ({len(sql)} total chars) ---\n")

        result = spark.sql(sql)
        rows = result.collect()
        print(f"  OK: {sorted(row['dst'] for row in rows)}")
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
