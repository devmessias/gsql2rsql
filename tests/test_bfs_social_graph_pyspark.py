"""Test: BFS via OpenCypher VLP em PySpark - Social Graph.

Grafo de teste (rede social com multiplos caminhos):

       Alice
      /  |  \\
    Bob Carol Dave
     \\  |   / |
       Evan    Frank
         |
       Grace

Arestas (KNOWS, direcionadas):
  Alice -> Bob, Alice -> Carol, Alice -> Dave
  Bob -> Evan, Carol -> Evan, Dave -> Evan, Dave -> Frank
  Evan -> Grace

Caracteristicas interessantes:
  - Multiplos caminhos para Evan (via Bob, Carol, ou Dave)
  - Dave tem 2 conexoes (Evan e Frank)
  - Frank e "folha" (sem saida)
  - Grace so e alcancavel via Evan
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("BFS_SocialGraph_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def social_graph(spark):
    """Create social network graph data."""
    # Nodes: people with different roles
    nodes_data = [
        ("Alice", "Person", "founder"),
        ("Bob", "Person", "employee"),
        ("Carol", "Person", "employee"),
        ("Dave", "Person", "manager"),
        ("Evan", "Person", "intern"),
        ("Frank", "Person", "contractor"),
        ("Grace", "Person", "intern"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "role"]
    )
    nodes_df.createOrReplaceTempView("people")

    # Edges: KNOWS relationships
    edges_data = [
        ("Alice", "Bob", "KNOWS"),
        ("Alice", "Carol", "KNOWS"),
        ("Alice", "Dave", "KNOWS"),
        ("Bob", "Evan", "KNOWS"),
        ("Carol", "Evan", "KNOWS"),
        ("Dave", "Evan", "KNOWS"),
        ("Dave", "Frank", "KNOWS"),
        ("Evan", "Grace", "KNOWS"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )
    edges_df.createOrReplaceTempView("connections")

    return {"nodes": nodes_df, "edges": edges_df}


@pytest.fixture(scope="module")
def graph_context(spark, social_graph):
    """Create GraphContext for social network."""
    graph = GraphContext(
        spark=spark,
        nodes_table="people",
        edges_table="connections",
        node_type_col="node_type",
        node_id_col="node_id",
        extra_node_attrs={"role": str},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return graph


class TestBFSSocialGraph:
    """BFS tests on social network graph."""

    def test_bfs_depth_1_direct_friends(self, spark, graph_context):
        """BFS depth 1: Alice -> {Bob, Carol, Dave}."""
        query = """
        MATCH (person)-[:KNOWS*1..1]->(friend)
        WHERE person.node_id = 'Alice'
        RETURN person.node_id AS person, friend.node_id AS friend
        ORDER BY friend
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS depth 1 (amigos diretos) ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        expected = 3
        assert len(rows) == expected, f"Alice tem {expected} amigos, got {len(rows)}"

        friends = {row["friend"] for row in rows}
        assert friends == {"Bob", "Carol", "Dave"}, f"Amigos: {friends}"

        print(f"BFS depth 1: Alice -> {friends}")

    def test_bfs_depth_2_friends_of_friends(self, spark, graph_context):
        """BFS depth 2: Alice pode alcancar Evan, Frank via depth 2."""
        query = """
        MATCH path = (person)-[:KNOWS*2..2]->(fof)
        WHERE person.node_id = 'Alice'
        RETURN person.node_id AS person, fof.node_id AS friend_of_friend
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS depth 2 (amigos de amigos) ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        # Alice -> Bob/Carol/Dave -> Evan (3 paths), Dave -> Frank (1 path)
        # Total: 4 paths at depth 2
        expected = 4
        assert len(rows) == expected, f"Esperado {expected} caminhos, got {len(rows)}"

        fof_names = {row["friend_of_friend"] for row in rows}
        assert "Evan" in fof_names, "Evan deve ser alcancavel em depth 2"
        assert "Frank" in fof_names, "Frank deve ser alcancavel em depth 2"

        print(f"BFS depth 2: {fof_names}")

    def test_bfs_multiple_paths_to_same_node(self, spark, graph_context):
        """Verificar multiplos caminhos para Evan."""
        query = """
        MATCH path = (alice)-[:KNOWS*2..2]->(evan)
        WHERE alice.node_id = 'Alice' AND evan.node_id = 'Evan'
        RETURN nodes(path) AS path_nodes
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para multiplos caminhos para Evan ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        # 3 caminhos: Alice->Bob->Evan, Alice->Carol->Evan, Alice->Dave->Evan
        expected = 3
        assert len(rows) == expected, f"Esperado {expected} caminhos, got {len(rows)}"

        paths = [tuple(row["path_nodes"]) for row in rows]
        expected_intermediates = {"Bob", "Carol", "Dave"}
        actual_intermediates = {path[1] for path in paths}

        assert actual_intermediates == expected_intermediates, \
            f"Intermediarios: {actual_intermediates}"

        print(f"Multiplos caminhos para Evan: {paths}")

    def test_bfs_depth_3_reaches_grace(self, spark, graph_context):
        """BFS depth 3: Alice -> ... -> Evan -> Grace."""
        query = """
        MATCH path = (alice)-[:KNOWS*3..3]->(grace)
        WHERE alice.node_id = 'Alice' AND grace.node_id = 'Grace'
        RETURN nodes(path) AS path_nodes, length(path) AS depth
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS depth 3 (alcancar Grace) ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        # 3 caminhos de depth 3 para Grace
        expected = 3
        assert len(rows) == expected, f"Esperado {expected} caminhos, got {len(rows)}"

        for row in rows:
            path = row["path_nodes"]
            assert path[0] == "Alice", "Deve comecar em Alice"
            assert path[-1] == "Grace", "Deve terminar em Grace"
            assert path[-2] == "Evan", "Penultimo deve ser Evan"
            assert row["depth"] == 3, "Depth deve ser 3"

        print(f"BFS depth 3: {len(rows)} caminhos ate Grace")

    def test_bfs_all_reachable_from_alice(self, spark, graph_context):
        """BFS depth 1-3: todos os nos alcancaveis de Alice."""
        query = """
        MATCH path = (alice)-[:KNOWS*1..3]->(reachable)
        WHERE alice.node_id = 'Alice'
        RETURN DISTINCT reachable.node_id AS reachable_node
        ORDER BY reachable_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para todos alcancaveis de Alice ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        reachable = {row["reachable_node"] for row in rows}
        expected = {"Bob", "Carol", "Dave", "Evan", "Frank", "Grace"}

        assert reachable == expected, f"Alcancaveis: {reachable}"

        print(f"Todos alcancaveis de Alice: {reachable}")

    def test_bfs_from_dave_two_branches(self, spark, graph_context):
        """BFS de Dave: tem 2 caminhos diretos (Evan e Frank)."""
        query = """
        MATCH (dave)-[:KNOWS*1..1]->(direct)
        WHERE dave.node_id = 'Dave'
        RETURN dave.node_id AS start, direct.node_id AS direct_connection
        ORDER BY direct_connection
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS de Dave ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        expected = 2
        assert len(rows) == expected, f"Dave tem {expected} conexoes, got {len(rows)}"

        connections = {row["direct_connection"] for row in rows}
        assert connections == {"Evan", "Frank"}, f"Conexoes: {connections}"

        print(f"BFS de Dave: {connections}")

    def test_bfs_frank_is_leaf(self, spark, graph_context):
        """BFS de Frank: e folha, nao tem conexoes de saida."""
        query = """
        MATCH (frank)-[:KNOWS*1..1]->(anyone)
        WHERE frank.node_id = 'Frank'
        RETURN frank.node_id AS start, anyone.node_id AS connection
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS de Frank (folha) ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 0, f"Frank e folha. Encontrado {len(rows)}"

        print("BFS de Frank: nenhuma conexao (correto - e folha)")

    def test_bfs_with_role_attribute(self, spark, graph_context):
        """BFS com atributo role dos nos."""
        query = """
        MATCH path = (alice)-[:KNOWS*1..2]->(contact)
        WHERE alice.node_id = 'Alice'
        RETURN contact.node_id AS name, contact.role AS role,
               length(path) AS distance
        ORDER BY distance, name
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS com atributo role ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        # Verificar roles
        role_map = {row["name"]: row["role"] for row in rows}

        # Depth 1: Bob, Carol, Dave
        assert role_map.get("Bob") == "employee"
        assert role_map.get("Carol") == "employee"
        assert role_map.get("Dave") == "manager"

        # Depth 2: Evan (intern), Frank (contractor)
        evan_rows = [r for r in rows if r["name"] == "Evan"]
        frank_rows = [r for r in rows if r["name"] == "Frank"]

        assert all(r["role"] == "intern" for r in evan_rows)
        assert all(r["role"] == "contractor" for r in frank_rows)

        print(f"BFS com roles: {role_map}")

    def test_bfs_count_paths_by_depth(self, spark, graph_context):
        """Contar caminhos por profundidade."""
        query = """
        MATCH path = (alice)-[:KNOWS*1..3]->(anyone)
        WHERE alice.node_id = 'Alice'
        RETURN length(path) AS depth, COUNT(*) AS path_count
        ORDER BY depth
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para contar caminhos por depth ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        depth_counts = {row["depth"]: row["path_count"] for row in rows}

        # Depth 1: 3 caminhos (Bob, Carol, Dave)
        # Depth 2: 4 caminhos (Evan x3, Frank x1)
        # Depth 3: 3 caminhos (Grace x3, via diferentes intermediarios)
        assert depth_counts.get(1) == 3, f"Depth 1: {depth_counts.get(1)}"
        assert depth_counts.get(2) == 4, f"Depth 2: {depth_counts.get(2)}"
        assert depth_counts.get(3) == 3, f"Depth 3: {depth_counts.get(3)}"

        print(f"Caminhos por depth: {depth_counts}")
