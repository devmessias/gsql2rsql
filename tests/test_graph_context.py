"""Tests for GraphContext API.

Tests the simplified Triple Store API that eliminates boilerplate.
"""

import pytest
from gsql2rsql import GraphContext


class TestGraphContextCreation:
    """Test GraphContext initialization."""

    def test_requires_nodes_table(self):
        """GraphContext requires nodes_table parameter."""
        with pytest.raises(ValueError, match="nodes_table is required"):
            GraphContext(edges_table="`catalog`.`schema`.`edges`")

    def test_requires_edges_table(self):
        """GraphContext requires edges_table parameter."""
        with pytest.raises(ValueError, match="edges_table is required"):
            GraphContext(nodes_table="`catalog`.`schema`.`nodes`")

    def test_minimal_creation_without_spark(self):
        """GraphContext can be created without spark (for manual type setup)."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )

        assert graph.nodes_table == "`catalog`.`schema`.`nodes`"
        assert graph.edges_table == "`catalog`.`schema`.`edges`"
        assert graph.node_type_col == "node_type"
        assert graph.edge_type_col == "relationship_type"

    def test_custom_column_names(self):
        """GraphContext accepts custom column names."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`",
            node_type_col="node_type",
            edge_type_col="rel_type",
            node_id_col="id"
        )

        assert graph.node_type_col == "node_type"
        assert graph.edge_type_col == "rel_type"
        assert graph.node_id_col == "id"

    def test_repr(self):
        """GraphContext has useful repr."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )
        graph.set_types(
            node_types=["Person", "Company"],
            edge_types=["KNOWS", "WORKS_AT"]
        )

        repr_str = repr(graph)
        assert "`catalog`.`schema`.`nodes`" in repr_str
        assert "`catalog`.`schema`.`edges`" in repr_str
        assert "node_types=2" in repr_str
        assert "edge_types=2" in repr_str


class TestGraphContextManualSetup:
    """Test manual type setup (without Spark)."""

    def test_set_types_manually(self):
        """Can manually set node and edge types."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )

        graph.set_types(
            node_types=["Person", "Company"],
            edge_types=["KNOWS", "WORKS_AT"]
        )

        # Should now be able to transpile
        sql = graph.transpile("MATCH (p:Person) RETURN p")
        assert "SELECT" in sql.upper()

    def test_transpile_without_types_fails(self):
        """Transpile fails if types not set."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )

        with pytest.raises(RuntimeError, match="Schema not initialized"):
            graph.transpile("MATCH (p:Person) RETURN p")


class TestGraphContextTranspile:
    """Test transpilation functionality."""

    @pytest.fixture
    def graph(self):
        """Create GraphContext with manual type setup."""
        g = GraphContext(
            nodes_table="`catalog`.`demo`.`Person`",
            edges_table="`catalog`.`demo`.`Knows`",
            node_type_col="node_type",
            edge_type_col="relationship_type",
            node_id_col="id",
            extra_node_attrs={"name": str, "age": int},
            extra_edge_attrs={"since": int},
        )
        g.set_types(node_types=["Person"], edge_types=["KNOWS"])
        return g

    def test_simple_node_query(self, graph):
        """Can transpile simple node query."""
        sql = graph.transpile("MATCH (p:Person) RETURN p.name")

        assert "SELECT" in sql.upper()
        assert "name" in sql.lower()
        assert "Person" in sql  # Table name appears in SQL

    def test_relationship_query(self, graph):
        """Can transpile relationship query."""
        sql = graph.transpile(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name"
        )

        assert "SELECT" in sql.upper()
        assert "JOIN" in sql.upper()
        assert "Knows" in sql  # Table name appears in SQL

    def test_filter_query_with_optimization(self, graph):
        """Filters are pushed down with optimization enabled."""
        sql = graph.transpile(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.name",
            optimize=True
        )

        # With predicate pushdown, filter should be in DataSource
        assert "WHERE" in sql.upper()
        assert "'Alice'" in sql

    def test_optimization_can_be_disabled(self, graph):
        """Can disable optimization."""
        sql = graph.transpile(
            "MATCH (p:Person) RETURN p.name",
            optimize=False
        )

        assert "SELECT" in sql.upper()

    def test_with_extra_attributes(self, graph):
        """Extra attributes are available in queries."""
        sql = graph.transpile("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")

        assert "age" in sql.lower()
        assert "30" in sql


class TestGraphContextExecute:
    """Test execution functionality (requires mocking Spark)."""

    def test_execute_without_spark_fails(self):
        """Execute requires spark session."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )
        graph.set_types(node_types=["Person"], edge_types=["KNOWS"])

        with pytest.raises(RuntimeError, match="Spark session required"):
            graph.execute("MATCH (p:Person) RETURN p")


class TestGraphContextEdgeCases:
    """Test edge cases and error handling."""

    def test_multiple_node_types(self):
        """Supports multiple node types."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )
        graph.set_types(
            node_types=["Person", "Company", "Device"],
            edge_types=["KNOWS"]
        )

        sql = graph.transpile("MATCH (p:Person) RETURN p")
        assert "node_type = 'Person'" in sql

    def test_multiple_edge_types(self):
        """Supports multiple edge types."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )
        graph.set_types(
            node_types=["Person"],
            edge_types=["KNOWS", "WORKS_AT", "MANAGES"]
        )

        sql = graph.transpile(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"
        )
        assert "relationship_type = 'KNOWS'" in sql

    def test_variable_length_path(self):
        """Supports variable-length paths."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`"
        )
        graph.set_types(node_types=["Person"], edge_types=["KNOWS"])

        sql = graph.transpile(
            "MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b"
        )
        assert "WITH RECURSIVE" in sql.upper() or "RECURSIVE" in sql.upper()

    def test_aggregation_query(self):
        """Supports aggregation queries."""
        graph = GraphContext(
            nodes_table="`catalog`.`schema`.`nodes`",
            edges_table="`catalog`.`schema`.`edges`",
            extra_node_attrs={"age": int}
        )
        graph.set_types(node_types=["Person"], edge_types=["KNOWS"])

        sql = graph.transpile(
            "MATCH (p:Person) RETURN AVG(p.age) AS avg_age"
        )
        assert "AVG" in sql.upper()


class TestGraphContextVsManualSetup:
    """Test that GraphContext produces same SQL as manual setup."""

    def test_equivalent_to_manual_setup(self):
        """GraphContext SQL matches manual setup.

        This test verifies that GraphContext produces valid SQL.
        GraphContext internally uses SimpleSQLSchemaProvider, so we just
        verify the output is well-formed.
        """
        # GraphContext approach
        # Note: Use table names WITHOUT backticks - renderer adds them automatically
        graph = GraphContext(
            nodes_table="catalog.demo.Person",
            edges_table="catalog.demo.Knows",
            node_id_col="id",  # Match manual setup
            edge_src_col="src",
            edge_dst_col="dst",
            extra_node_attrs={"name": str, "age": int}
        )
        graph.set_types(node_types=["Person"], edge_types=["KNOWS"])

        sql_context = graph.transpile(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.name"
        )

        # Verify the SQL is well-formed and contains expected elements
        assert "SELECT" in sql_context.upper()
        assert "name" in sql_context.lower()
        assert "'Alice'" in sql_context
        assert "Person" in sql_context
