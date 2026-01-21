"""Test: Wildcard node uses correct node_id column (not hardcoded 'id').

Verifica que o transpiler usa o node_id_col correto do wildcard schema
tanto para queries VLP quanto para queries simples com nós sem label.

O teste com nome "maluco" (xpto_banana_id_42) força a detecção de
qualquer valor hardcoded "id" no código.
"""

import pytest
from gsql2rsql import GraphContext


class TestWildcardNodeIdColumn:
    """Testes para verificar uso correto da coluna node_id em wildcard nodes."""

    @pytest.fixture
    def graph_with_node_id(self):
        """GraphContext com node_id_col='node_id' (default)."""
        graph = GraphContext(
            nodes_table="catalog.schema.nodes",
            edges_table="catalog.schema.edges",
            node_id_col="node_id",  # Explícito para clareza
            extra_node_attrs={"name": str, "type": str}
        )
        graph.set_types(
            node_types=["Person", "Company"],
            edge_types=["KNOWS", "WORKS_AT"]
        )
        return graph

    @pytest.fixture
    def graph_with_maluco_id(self):
        """GraphContext com node_id_col='xpto_banana_id_42' (nome maluco).

        Esse nome força detecção de qualquer 'id' hardcoded no código.
        """
        graph = GraphContext(
            nodes_table="catalog.schema.nodes",
            edges_table="catalog.schema.edges",
            node_id_col="xpto_banana_id_42",  # NOME MALUCO!
            extra_node_attrs={"name": str, "type": str}
        )
        graph.set_types(
            node_types=["Person", "Company"],
            edge_types=["KNOWS", "WORKS_AT"]
        )
        return graph

    # =========================================================================
    # TESTES COM NOME MALUCO (xpto_banana_id_42) - DETECTA HARDCODED
    # =========================================================================

    def test_simple_match_maluco_id_source_unlabeled(self, graph_with_maluco_id):
        """MATCH simples com source sem label deve usar xpto_banana_id_42."""
        query = """
        MATCH (a)-[:WORKS_AT]->(c:Company)
        RETURN a.xpto_banana_id_42 AS src_id, c.xpto_banana_id_42 AS dst_id
        """
        sql = graph_with_maluco_id.transpile(query)

        # Verificar que usa xpto_banana_id_42
        assert "xpto_banana_id_42" in sql, "SQL deve usar 'xpto_banana_id_42'"
        assert "_gsql2rsql_a_xpto_banana_id_42" in sql

        # Verificar que NÃO há .id isolado
        for line in sql.split('\n'):
            if '.id ' in line or '.id=' in line or '.id)' in line:
                # Garantir que não é parte de xpto_banana_id_42
                if 'xpto_banana_id_42' not in line:
                    pytest.fail(f"Hardcoded .id encontrado: {line.strip()}")

    def test_vlp_maluco_id_both_unlabeled(self, graph_with_maluco_id):
        """VLP com ambos sem label deve usar xpto_banana_id_42 em TODOS os lugares."""
        query = """
        MATCH (origem)-[:KNOWS*1..2]->(destino)
        RETURN origem.xpto_banana_id_42 AS src, destino.xpto_banana_id_42 AS dst
        """
        sql = graph_with_maluco_id.transpile(query)

        # Verificar colunas projetadas
        assert "_gsql2rsql_origem_xpto_banana_id_42" in sql, \
            "VLP deve projetar _gsql2rsql_origem_xpto_banana_id_42"
        assert "_gsql2rsql_destino_xpto_banana_id_42" in sql, \
            "VLP deve projetar _gsql2rsql_destino_xpto_banana_id_42"

        # Verificar JOINs usam xpto_banana_id_42
        assert "source.xpto_banana_id_42" in sql, "JOIN com source deve usar xpto_banana_id_42"
        assert "sink.xpto_banana_id_42" in sql, "JOIN com sink deve usar xpto_banana_id_42"

    def test_vlp_maluco_id_with_inline_filter(self, graph_with_maluco_id):
        """VLP com filtro inline deve usar xpto_banana_id_42 no base case JOIN."""
        query = """
        MATCH (origem{type: "test"})-[:KNOWS*1..3]->(destino)
        RETURN origem.xpto_banana_id_42 AS src, destino.xpto_banana_id_42 AS dst
        """
        sql = graph_with_maluco_id.transpile(query)

        # Base case JOIN deve usar xpto_banana_id_42
        assert "src.xpto_banana_id_42" in sql or "source.xpto_banana_id_42" in sql, \
            "Base case JOIN deve usar xpto_banana_id_42"

        # Projeções devem usar xpto_banana_id_42
        assert "_gsql2rsql_origem_xpto_banana_id_42" in sql
        assert "_gsql2rsql_destino_xpto_banana_id_42" in sql

    # =========================================================================
    # TESTES COM node_id (DEFAULT)
    # =========================================================================

    def test_simple_match_unlabeled_source_uses_node_id(self, graph_with_node_id):
        """MATCH simples com source sem label deve usar node_id, não 'id'."""
        query = """
        MATCH (a)-[:WORKS_AT]->(c:Company)
        RETURN a.node_id AS src_id, c.node_id AS dst_id
        """
        sql = graph_with_node_id.transpile(query)

        # Verificar que usa node_id
        assert "node_id" in sql, "SQL deve usar 'node_id'"
        assert "_gsql2rsql_a_node_id" in sql

    def test_simple_match_unlabeled_target_uses_node_id(self, graph_with_node_id):
        """MATCH simples com target sem label deve usar node_id."""
        query = """
        MATCH (p:Person)-[:KNOWS]->(target)
        RETURN p.node_id AS src_id, target.node_id AS dst_id
        """
        sql = graph_with_node_id.transpile(query)

        assert "node_id" in sql, "SQL deve usar 'node_id'"
        assert "_gsql2rsql_target_node_id" in sql

    def test_simple_match_both_unlabeled_uses_node_id(self, graph_with_node_id):
        """MATCH simples com ambos sem label deve usar node_id."""
        query = """
        MATCH (a)-[:KNOWS]->(b)
        RETURN a.node_id AS src_id, b.node_id AS dst_id
        """
        sql = graph_with_node_id.transpile(query)

        assert "_gsql2rsql_a_node_id" in sql, "SQL deve ter _gsql2rsql_a_node_id"
        assert "_gsql2rsql_b_node_id" in sql, "SQL deve ter _gsql2rsql_b_node_id"

    def test_vlp_unlabeled_source_uses_node_id(self, graph_with_node_id):
        """VLP com source sem label deve usar node_id em TODOS os lugares."""
        query = """
        MATCH (origem)-[:KNOWS*1..2]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        """
        sql = graph_with_node_id.transpile(query)

        # Verificar colunas projetadas
        assert "_gsql2rsql_origem_node_id" in sql
        assert "_gsql2rsql_destino_node_id" in sql

        # Verificar JOINs usam node_id
        assert "source.node_id" in sql, "JOIN com source deve usar node_id"
        assert "sink.node_id" in sql, "JOIN com sink deve usar node_id"

    def test_vlp_unlabeled_target_uses_node_id(self, graph_with_node_id):
        """VLP com target sem label deve usar node_id."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..2]->(b)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        sql = graph_with_node_id.transpile(query)

        assert "_gsql2rsql_b_node_id" in sql, "VLP deve projetar _gsql2rsql_b_node_id"

    def test_vlp_both_unlabeled_uses_node_id(self, graph_with_node_id):
        """VLP com ambos sem label deve usar node_id."""
        query = """
        MATCH path = (a)-[:KNOWS*1..3]->(b)
        RETURN a.node_id, b.node_id, length(path) AS hops
        """
        sql = graph_with_node_id.transpile(query)

        assert "_gsql2rsql_a_node_id" in sql
        assert "_gsql2rsql_b_node_id" in sql

    def test_vlp_with_inline_filter_uses_node_id(self, graph_with_node_id):
        """VLP com filtro inline deve usar node_id corretamente."""
        query = """
        MATCH (origem{type: "example"})-[:KNOWS*1..3]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        """
        sql = graph_with_node_id.transpile(query)

        # Base case JOIN deve usar node_id
        assert "src.node_id" in sql or "source.node_id" in sql, \
            "Base case JOIN deve usar node_id"

        # Projeções devem usar node_id
        assert "_gsql2rsql_origem_node_id" in sql
        assert "_gsql2rsql_destino_node_id" in sql
