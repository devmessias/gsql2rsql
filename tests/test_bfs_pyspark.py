"""Test: BFS (Breadth-First Search) via OpenCypher VLP em PySpark.

Cria um grafo em árvore e verifica que VLP retorna os caminhos corretos.

Grafo de teste (árvore):

       A (root)
      / \
     B   C
    /|   |\
   D E   F G

Arestas (direcionadas):
  A -> B, A -> C
  B -> D, B -> E
  C -> F, C -> G
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("BFS_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create tree graph data."""
    # Vertices: A, B, C, D, E, F, G
    nodes_data = [
        ("A", "Root", "root"),
        ("B", "Child", "level1"),
        ("C", "Child", "level1"),
        ("D", "Leaf", "level2"),
        ("E", "Leaf", "level2"),
        ("F", "Leaf", "level2"),
        ("G", "Leaf", "level2"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "level"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Edges: parent -> child
    edges_data = [
        ("A", "B", "PARENT_OF"),
        ("A", "C", "PARENT_OF"),
        ("B", "D", "PARENT_OF"),
        ("B", "E", "PARENT_OF"),
        ("C", "F", "PARENT_OF"),
        ("C", "G", "PARENT_OF"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )
    edges_df.createOrReplaceTempView("edges")

    return {"nodes": nodes_df, "edges": edges_df}


@pytest.fixture(scope="module")
def graph_context(spark, graph_data):
    """Create GraphContext for the tree graph."""
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        extra_node_attrs={"level": str},
    )
    graph.set_types(
        node_types=["Root", "Child", "Leaf"],
        edge_types=["PARENT_OF"],
    )
    return graph


class TestBFSViaCypher:
    """Testes de BFS usando OpenCypher VLP."""

    def test_bfs_depth_1_from_root(self, spark, graph_context):
        """BFS depth 1: A -> {B, C}."""
        query = """
        MATCH (root)-[:PARENT_OF*1..1]->(child)
        WHERE root.node_id = 'A'
        RETURN root.node_id AS start, child.node_id AS end_node
        ORDER BY end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS depth 1 ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert result is not None, "DataFrame não deve ser nulo"
        assert len(rows) == 2, f"Esperado 2 filhos de A, encontrado {len(rows)}"

        children = {row["end_node"] for row in rows}
        assert children == {"B", "C"}, f"Filhos de A devem ser B e C, encontrado {children}"

        print(f"✅ BFS depth 1: A -> {children}")

    def test_bfs_depth_2_from_root(self, spark, graph_context):
        """BFS depth 2: A -> {B, C, D, E, F, G}."""
        query = """
        MATCH path = (root)-[:PARENT_OF*1..2]->(descendant)
        WHERE root.node_id = 'A'
        RETURN root.node_id AS start, descendant.node_id AS end_node, length(path) AS depth
        ORDER BY depth, end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS depth 2 ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert result is not None, "DataFrame não deve ser nulo"
        assert len(rows) == 6, f"Esperado 6 descendentes de A, encontrado {len(rows)}"

        # Verificar por depth
        depth_1 = {row["end_node"] for row in rows if row["depth"] == 1}
        depth_2 = {row["end_node"] for row in rows if row["depth"] == 2}

        assert depth_1 == {"B", "C"}, f"Depth 1 deve ser {{B, C}}, encontrado {depth_1}"
        assert depth_2 == {"D", "E", "F", "G"}, f"Depth 2 deve ser {{D, E, F, G}}, encontrado {depth_2}"

        print(f"✅ BFS depth 1: {depth_1}")
        print(f"✅ BFS depth 2: {depth_2}")

    def test_bfs_specific_paths(self, spark, graph_context):
        """Verificar caminhos específicos: A->B->D, A->B->E, A->C->F, A->C->G."""
        query = """
        MATCH path = (root)-[:PARENT_OF*2..2]->(leaf)
        WHERE root.node_id = 'A'
        RETURN nodes(path) AS path_nodes
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para paths específicos ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert result is not None, "DataFrame não deve ser nulo"
        assert len(rows) == 4, f"Esperado 4 caminhos de tamanho 2, encontrado {len(rows)}"

        # Extrair caminhos
        paths = [tuple(row["path_nodes"]) for row in rows]
        expected_paths = [
            ("A", "B", "D"),
            ("A", "B", "E"),
            ("A", "C", "F"),
            ("A", "C", "G"),
        ]

        for expected in expected_paths:
            assert expected in paths, f"Caminho {expected} não encontrado em {paths}"

        print(f"✅ Caminhos encontrados: {paths}")

    def test_bfs_from_intermediate_node(self, spark, graph_context):
        """BFS a partir de nó intermediário: B -> {D, E}."""
        query = """
        MATCH (start)-[:PARENT_OF*1..1]->(child)
        WHERE start.node_id = 'B'
        RETURN start.node_id AS start, child.node_id AS end_node
        ORDER BY end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS de B ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert result is not None
        assert len(rows) == 2, f"B tem 2 filhos, encontrado {len(rows)}"

        children = {row["end_node"] for row in rows}
        assert children == {"D", "E"}, f"Filhos de B devem ser D e E, encontrado {children}"

        print(f"✅ BFS de B: {children}")

    def test_bfs_leaf_node_no_children(self, spark, graph_context):
        """BFS de nó folha deve retornar vazio."""
        query = """
        MATCH (leaf)-[:PARENT_OF*1..1]->(child)
        WHERE leaf.node_id = 'D'
        RETURN leaf.node_id AS start, child.node_id AS end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS de folha D ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert result is not None
        assert len(rows) == 0, f"D é folha, não deve ter filhos. Encontrado {len(rows)}"

        print("✅ BFS de folha D: nenhum filho (correto)")

    def test_bfs_unlabeled_nodes(self, spark, graph_context):
        """BFS com nós sem label (wildcard)."""
        query = """
        MATCH (start)-[:PARENT_OF*1..2]->(end_node)
        WHERE start.node_id = 'A'
        RETURN start.node_id AS root, end_node.node_id AS descendant, end_node.level AS level
        ORDER BY level, descendant
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL para BFS sem labels ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert result is not None
        assert len(rows) == 6, f"Esperado 6 descendentes, encontrado {len(rows)}"

        # Verificar levels
        level1_nodes = {row["descendant"] for row in rows if row["level"] == "level1"}
        level2_nodes = {row["descendant"] for row in rows if row["level"] == "level2"}

        assert level1_nodes == {"B", "C"}
        assert level2_nodes == {"D", "E", "F", "G"}

        print(f"✅ Level 1: {level1_nodes}")
        print(f"✅ Level 2: {level2_nodes}")
