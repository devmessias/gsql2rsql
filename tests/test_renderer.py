"""Tests for the Databricks SQL renderer."""

import pytest

from gsql2rsql.parser.ast import (
    QueryExpressionValue,
    QueryExpressionProperty,
    RelationshipEntity,
    RelationshipDirection,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    ProjectionOperator,
    SelectionOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.schema import (
    EntityField,
    Schema,
    ValueField,
)
from gsql2rsql.renderer.sql_renderer import (
    SQLRenderer,
    DatabricksSqlType,
    TYPE_TO_SQL_TYPE,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
)


class TestSQLRenderer:
    """Tests for SQLRenderer (Databricks SQL)."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_table(
            SQLTableDescriptor(
                entity_id="Person",
                table_name="Person",
                node_id_columns=["id"],
            )
        )
        self.schema.add_table(
            SQLTableDescriptor(
                entity_id="Movie",
                table_name="Movie",
                node_id_columns=["id"],
            )
        )
        self.schema.add_table(
            SQLTableDescriptor(
                entity_id="Person@ACTED_IN@Movie",
                table_name="ActedIn",
                node_id_columns=["person_id", "movie_id"],
            )
        )

        self.renderer = SQLRenderer(
            db_schema_provider=self.schema,
        )

    def test_render_literal_value_string(self) -> None:
        """Test rendering string literal values (Databricks syntax)."""
        expr = QueryExpressionValue(value="hello", value_type=str)
        result = self.renderer._render_value(expr)
        # Databricks uses simple quotes, not N'' prefix
        assert result == "'hello'"

    def test_render_literal_value_escaped_string(self) -> None:
        """Test rendering strings with quotes."""
        expr = QueryExpressionValue(value="it's", value_type=str)
        result = self.renderer._render_value(expr)
        assert result == "'it''s'"

    def test_render_literal_value_integer(self) -> None:
        """Test rendering integer values."""
        expr = QueryExpressionValue(value=42, value_type=int)
        result = self.renderer._render_value(expr)
        assert result == "42"

    def test_render_literal_value_float(self) -> None:
        """Test rendering float values."""
        expr = QueryExpressionValue(value=3.14, value_type=float)
        result = self.renderer._render_value(expr)
        assert result == "3.14"

    def test_render_literal_value_boolean_true(self) -> None:
        """Test rendering boolean TRUE (Databricks syntax)."""
        expr = QueryExpressionValue(value=True, value_type=bool)
        result = self.renderer._render_value(expr)
        # Databricks uses TRUE/FALSE, not 1/0
        assert result == "TRUE"

    def test_render_literal_value_boolean_false(self) -> None:
        """Test rendering boolean FALSE (Databricks syntax)."""
        expr = QueryExpressionValue(value=False, value_type=bool)
        result = self.renderer._render_value(expr)
        assert result == "FALSE"

    def test_render_literal_value_null(self) -> None:
        """Test rendering NULL values."""
        expr = QueryExpressionValue(value=None, value_type=type(None))
        result = self.renderer._render_value(expr)
        assert result == "NULL"

    def test_render_property_expression(self) -> None:
        """Test rendering property expressions."""
        expr = QueryExpressionProperty(
            variable_name="p",
            property_name="name",
        )
        # The renderer generates internal field names with _gsql2rsql_ prefix
        result = self.renderer._get_field_name(expr.variable_name, expr.property_name)
        assert result == "_gsql2rsql_p_name"


class TestDatabricksSqlTypes:
    """Tests for Databricks SQL type mappings."""

    def test_type_mapping(self) -> None:
        """Test Python to Databricks SQL type mapping."""
        assert TYPE_TO_SQL_TYPE[int] == DatabricksSqlType.BIGINT
        assert TYPE_TO_SQL_TYPE[float] == DatabricksSqlType.DOUBLE
        assert TYPE_TO_SQL_TYPE[str] == DatabricksSqlType.STRING
        assert TYPE_TO_SQL_TYPE[bool] == DatabricksSqlType.BOOLEAN
        assert TYPE_TO_SQL_TYPE[bytes] == DatabricksSqlType.BINARY
        assert TYPE_TO_SQL_TYPE[list] == DatabricksSqlType.ARRAY
        assert TYPE_TO_SQL_TYPE[dict] == DatabricksSqlType.MAP


class TestSQLTableDescriptor:
    """Tests for SQL table descriptor (Databricks format)."""

    def test_full_table_name_with_schema(self) -> None:
        """Test full table name with schema - returns as-is without quoting."""
        descriptor = SQLTableDescriptor(
            table_or_view_name="users",
            schema_name="catalog.schema",
        )
        # Identifiers are returned as-is - user is responsible for backticks
        assert descriptor.full_table_name == "catalog.schema.users"

    def test_full_table_name_without_schema(self) -> None:
        """Test full table name without schema - simple identifiers are not quoted."""
        descriptor = SQLTableDescriptor(
            table_or_view_name="users",
            schema_name="",
        )
        # 'users' is a simple identifier, no quoting needed
        assert descriptor.full_table_name == "users"

    def test_full_table_name_dbo_schema_skipped(self) -> None:
        """Test that 'dbo' schema is skipped (SQL Server convention, not Databricks)."""
        descriptor = SQLTableDescriptor(
            table_or_view_name="users",
            schema_name="dbo",
        )
        # dbo is skipped for Databricks compatibility
        assert descriptor.full_table_name == "users"

    def test_full_table_name_with_special_chars(self) -> None:
        """Test that identifiers are returned as-is even with special characters."""
        descriptor = SQLTableDescriptor(
            table_or_view_name="`user table`",  # User provides backticks
            schema_name="",
        )
        # Returns exactly what user provided
        assert descriptor.full_table_name == "`user table`"

    def test_parse_table_name_with_schema(self) -> None:
        """Test parsing table name with schema."""
        descriptor = SQLTableDescriptor(
            entity_id="users",
            table_name="catalog.schema.users",
        )
        assert descriptor.schema_name == "catalog.schema"
        assert descriptor.table_or_view_name == "users"

    def test_parse_table_name_without_schema(self) -> None:
        """Test parsing table name without schema."""
        descriptor = SQLTableDescriptor(
            entity_id="users",
            table_name="users",
        )
        assert descriptor.schema_name == ""
        assert descriptor.table_or_view_name == "users"


class TestSchemaRendering:
    """Tests for schema-related rendering."""

    def test_value_field(self) -> None:
        """Test ValueField creation and properties."""
        field = ValueField(field_alias="total", data_type=int)
        assert field.field_alias == "total"
        assert field.data_type == int

    def test_entity_field(self) -> None:
        """Test EntityField creation and properties."""
        field = EntityField(field_alias="p", entity_name="Person")
        assert field.field_alias == "p"
        assert field.entity_name == "Person"

    def test_schema_field_lookup(self) -> None:
        """Test Schema field lookup by alias."""
        schema = Schema()
        field1 = ValueField(field_alias="name", data_type=str)
        field2 = EntityField(field_alias="p", entity_name="Person")
        schema.add_field(field1)
        schema.add_field(field2)

        result = schema.get_field("name")
        assert result is field1

        result = schema.get_field("p")
        assert result is field2

        result = schema.get_field("unknown")
        assert result is None


class TestRecursiveTraversalOperator:
    """Tests for recursive traversal operator."""

    def test_create_recursive_operator(self) -> None:
        """Test creating a recursive traversal operator."""
        op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=5,
        )
        assert op.edge_types == ["KNOWS"]
        assert op.source_node_type == "Person"
        assert op.target_node_type == "Person"
        assert op.min_hops == 1
        assert op.max_hops == 5

    def test_recursive_operator_str(self) -> None:
        """Test string representation of recursive operator."""
        op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
        )
        result = str(op)
        assert "RecursiveTraversal" in result
        assert "KNOWS" in result
        assert "*1..3" in result

    def test_recursive_operator_multiple_edge_types(self) -> None:
        """Test recursive operator with multiple edge types."""
        op = RecursiveTraversalOperator(
            edge_types=["KNOWS", "FOLLOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
        )
        assert op.edge_types == ["KNOWS", "FOLLOWS"]
        result = str(op)
        assert "KNOWS|FOLLOWS" in result


class TestVariableLengthRelationship:
    """Tests for variable-length relationship patterns."""

    def test_fixed_length_relationship(self) -> None:
        """Test a fixed-length (non-variable) relationship."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.FORWARD,
        )
        assert not rel.is_variable_length
        assert "[r:KNOWS]->" in str(rel)

    def test_variable_length_relationship_bounded(self) -> None:
        """Test a variable-length relationship with bounds."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.FORWARD,
            min_hops=1,
            max_hops=5,
        )
        assert rel.is_variable_length
        assert "*1..5" in str(rel)

    def test_variable_length_relationship_min_only(self) -> None:
        """Test a variable-length relationship with min only."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.FORWARD,
            min_hops=2,
            max_hops=None,
        )
        assert rel.is_variable_length
        assert "*2.." in str(rel)

    def test_variable_length_relationship_max_only(self) -> None:
        """Test a variable-length relationship with max only."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.FORWARD,
            min_hops=None,
            max_hops=3,
        )
        assert rel.is_variable_length
        assert "*..3" in str(rel)


class TestBFSWithRecursive:
    """Tests for BFS traversal with WITH RECURSIVE."""

    def setup_method(self) -> None:
        """Set up test fixtures for BFS tests."""
        from gsql2rsql.common.schema import EntityProperty

        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(
                table_name="graph.Person",
                node_id_columns=["id"],
            ),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def test_bfs_generates_with_recursive(self) -> None:
        """Test that BFS query generates WITH RECURSIVE CTE."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) RETURN b.id"
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should contain WITH RECURSIVE
        assert "WITH RECURSIVE" in sql

    def test_bfs_uses_correct_edge_columns(self) -> None:
        """Test that BFS uses correct source_id and target_id columns."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.id"
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should use source_id and target_id columns
        assert "source_id" in sql
        assert "target_id" in sql

    def test_bfs_has_cycle_detection(self) -> None:
        """Test that BFS includes cycle detection."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) RETURN b.id"
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should have ARRAY_CONTAINS for cycle detection
        assert "ARRAY_CONTAINS" in sql

    def test_bfs_respects_max_depth(self) -> None:
        """Test that BFS respects max depth limit."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = "MATCH (a:Person)-[:KNOWS*1..7]->(b:Person) RETURN b.id"
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should have depth < 7 check
        assert "depth < 7" in sql

    def test_bfs_join_has_proper_condition(self) -> None:
        """Test that final JOIN has proper condition (not ON TRUE)."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.id"
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should NOT have ON TRUE (empty join condition)
        # The join should connect end_node to Person.id
        assert "ON\n    TRUE" not in sql
        # Should have proper join condition
        assert "end_node" in sql or "= p.end_node" in sql

    def test_bfs_projects_target_node_properties(self) -> None:
        """Test that BFS properly projects target node properties."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        RETURN b.id AS id, b.name AS name
        """
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should select id and name from Person table
        assert "AS id" in sql
        # Should have valid SELECT (not empty)
        assert "SELECT\n  FROM" not in sql


class TestBFSMultipleEdgeTypes:
    """Tests for BFS with multiple edge types (e.g., [:TYPE_A|TYPE_B*1..3]).

    This tests the generic case where a traversal can follow multiple
    relationship types. The edge type names (KNOWS, FOLLOWS) are just
    examples - the implementation should work with any edge type names
    defined in the schema.
    """

    def setup_method(self) -> None:
        """Set up test fixtures with multiple edge types."""
        from gsql2rsql.common.schema import EntityProperty

        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.edges", filter="edge_type = 'KNOWS'"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="FOLLOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(
                table_name="graph.edges", filter="edge_type = 'FOLLOWS'"
            ),
        )

    def test_bfs_with_multiple_edge_types(self) -> None:
        """Test BFS traversal with multiple edge types (KNOWS|FOLLOWS)."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        # Cypher query with two possible edge types
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        RETURN DISTINCT f.name
        """
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should generate SQL with UNION of both edge types
        assert "WITH RECURSIVE" in sql
        # Should filter by both edge types (OR condition or UNION)
        assert "KNOWS" in sql
        assert "FOLLOWS" in sql

    def test_bfs_multiple_edges_uses_union_or_filter(self) -> None:
        """Test that multiple edge types use UNION or OR filter."""
        from gsql2rsql import (
            OpenCypherParser,
            LogicalPlan,
            SQLRenderer,
        )

        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        RETURN f.id
        """
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Expected: edge_type IN ('KNOWS', 'FOLLOWS')
        # OR: UNION of two CTEs
        has_in_clause = "IN ('KNOWS', 'FOLLOWS')" in sql
        has_union = "UNION" in sql and "KNOWS" in sql and "FOLLOWS" in sql
        assert (
            has_in_clause or has_union
        ), "Expected IN clause or UNION for multiple edge types"
