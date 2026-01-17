"""Abstract Syntax Tree (AST) nodes for openCypher queries."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Iterator

from gsql2rsql.common.utils import change_indentation
from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperator,
    BinaryOperatorInfo,
    Function,
)


class TreeNode(ABC):
    """Base class for all AST nodes."""

    @property
    @abstractmethod
    def children(self) -> list[TreeNode]:
        """Return the children of this node."""
        ...

    def dump_tree(self, depth: int = 0) -> str:
        """
        Dump the tree in textual format for debugging.

        Args:
            depth: Current depth for indentation.

        Returns:
            String representation of the tree.
        """
        lines = [
            change_indentation(f"+{self.__class__.__name__}", depth),
            change_indentation(f"|{self}", depth),
        ]
        for child in self.children:
            lines.append(child.dump_tree(depth + 1))
        return "\n".join(lines)

    def get_children_of_type[T: TreeNode](self, node_type: type[T]) -> Iterator[T]:
        """
        Get all descendants of a specific type.

        Args:
            node_type: The type of nodes to find.

        Yields:
            Nodes of the specified type.
        """
        if isinstance(self, node_type):
            yield self
        for child in self.children:
            yield from child.get_children_of_type(node_type)


# ==============================================================================
# Query Expression Nodes
# ==============================================================================


@dataclass
class QueryExpression(TreeNode, ABC):
    """Base class for all query expressions."""

    @property
    def children(self) -> list[TreeNode]:
        return []

    @abstractmethod
    def evaluate_type(self) -> type[Any] | None:
        """
        Static evaluation of the returned data type.

        Returns:
            Python type equivalent of the data type.
        """
        ...

    def get_children_query_expression_type[T: QueryExpression](
        self, expr_type: type[T]
    ) -> Iterator[T]:
        """Get all descendant expressions of a specific type."""
        return self.get_children_of_type(expr_type)


@dataclass
class QueryExpressionValue(QueryExpression):
    """A literal value expression."""

    value: Any
    value_type: type[Any]

    @property
    def children(self) -> list[TreeNode]:
        return []

    def evaluate_type(self) -> type[Any] | None:
        return self.value_type

    def __str__(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return f"'{self.value}'"
        if isinstance(self.value, bool):
            return str(self.value).lower()
        return str(self.value)


@dataclass
class QueryExpressionProperty(QueryExpression):
    """A property access expression (e.g., n.name)."""

    variable_name: str
    property_name: str | None = None
    data_type: type[Any] | None = None

    @property
    def children(self) -> list[TreeNode]:
        return []

    def evaluate_type(self) -> type[Any] | None:
        return self.data_type

    def __str__(self) -> str:
        if self.property_name:
            return f"{self.variable_name}.{self.property_name}"
        return self.variable_name


@dataclass
class QueryExpressionBinary(QueryExpression):
    """A binary expression (e.g., a + b)."""

    operator: BinaryOperatorInfo | None = None
    left_expression: QueryExpression | None = None
    right_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = []
        if self.left_expression:
            result.append(self.left_expression)
        if self.right_expression:
            result.append(self.right_expression)
        return result

    def evaluate_type(self) -> type[Any] | None:
        if not self.operator:
            return None
        from gsql2rsql.parser.operators import BinaryOperatorType

        if self.operator.operator_type == BinaryOperatorType.LOGICAL:
            return bool
        if self.operator.operator_type == BinaryOperatorType.COMPARISON:
            return bool
        # For value operators, return the type of the operands
        if self.left_expression:
            return self.left_expression.evaluate_type()
        return None

    def __str__(self) -> str:
        op_str = self.operator.name.name if self.operator else "?"
        return f"({self.left_expression} {op_str} {self.right_expression})"


@dataclass
class QueryExpressionFunction(QueryExpression):
    """A function call expression."""

    function: Function
    parameters: list[QueryExpression] = field(default_factory=list)
    data_type: type[Any] | None = None

    @property
    def children(self) -> list[TreeNode]:
        return list(self.parameters)

    def evaluate_type(self) -> type[Any] | None:
        return self.data_type

    def __str__(self) -> str:
        params = ", ".join(str(p) for p in self.parameters)
        return f"{self.function.name}({params})"


@dataclass
class QueryExpressionAggregationFunction(QueryExpression):
    """An aggregation function call expression."""

    aggregation_function: AggregationFunction
    is_distinct: bool = False
    inner_expression: QueryExpression | None = None
    data_type: type[Any] | None = None

    @property
    def children(self) -> list[TreeNode]:
        if self.inner_expression:
            return [self.inner_expression]
        return []

    def evaluate_type(self) -> type[Any] | None:
        return self.data_type

    def __str__(self) -> str:
        distinct = "DISTINCT " if self.is_distinct else ""
        inner = str(self.inner_expression) if self.inner_expression else "*"
        return f"{self.aggregation_function.name}({distinct}{inner})"


@dataclass
class QueryExpressionList(QueryExpression):
    """A list expression (e.g., [1, 2, 3])."""

    items: list[QueryExpression] = field(default_factory=list)

    @property
    def children(self) -> list[TreeNode]:
        return list(self.items)

    def evaluate_type(self) -> type[Any] | None:
        return list

    def __str__(self) -> str:
        items = ", ".join(str(i) for i in self.items)
        return f"[{items}]"


@dataclass
class QueryExpressionCaseExpression(QueryExpression):
    """A CASE expression."""

    test_expression: QueryExpression | None = None
    alternatives: list[tuple[QueryExpression, QueryExpression]] = field(
        default_factory=list
    )
    else_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = []
        if self.test_expression:
            result.append(self.test_expression)
        for when_expr, then_expr in self.alternatives:
            result.extend([when_expr, then_expr])
        if self.else_expression:
            result.append(self.else_expression)
        return result

    def evaluate_type(self) -> type[Any] | None:
        if self.alternatives:
            return self.alternatives[0][1].evaluate_type()
        if self.else_expression:
            return self.else_expression.evaluate_type()
        return None

    def __str__(self) -> str:
        parts = ["CASE"]
        if self.test_expression:
            parts.append(str(self.test_expression))
        for when_expr, then_expr in self.alternatives:
            parts.append(f"WHEN {when_expr} THEN {then_expr}")
        if self.else_expression:
            parts.append(f"ELSE {self.else_expression}")
        parts.append("END")
        return " ".join(parts)


@dataclass
class QueryExpressionWithAlias(QueryExpression):
    """An expression with an alias (e.g., n.name AS name)."""

    inner_expression: QueryExpression
    alias: str

    @property
    def children(self) -> list[TreeNode]:
        return [self.inner_expression]

    def evaluate_type(self) -> type[Any] | None:
        return self.inner_expression.evaluate_type()

    def __str__(self) -> str:
        return f"{self.inner_expression} AS {self.alias}"


# ==============================================================================
# Query Structure Nodes
# ==============================================================================


class SortOrder(Enum):
    """Sort order for ORDER BY clauses."""

    ASC = auto()
    DESC = auto()


@dataclass
class SortItem(TreeNode):
    """A single item in an ORDER BY clause."""

    expression: QueryExpression
    order: SortOrder = SortOrder.ASC

    @property
    def children(self) -> list[TreeNode]:
        return [self.expression]

    def __str__(self) -> str:
        order_str = "DESC" if self.order == SortOrder.DESC else "ASC"
        return f"{self.expression} {order_str}"


@dataclass
class LimitClause(TreeNode):
    """A LIMIT clause."""

    limit_expression: QueryExpression
    skip_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = [self.limit_expression]
        if self.skip_expression:
            result.append(self.skip_expression)
        return result

    def __str__(self) -> str:
        skip_part = f"SKIP {self.skip_expression} " if self.skip_expression else ""
        return f"{skip_part}LIMIT {self.limit_expression}"


# ==============================================================================
# Entity Nodes (Nodes and Relationships)
# ==============================================================================


@dataclass
class Entity(TreeNode, ABC):
    """Base class for graph entities (nodes and relationships)."""

    alias: str
    entity_name: str

    @property
    def children(self) -> list[TreeNode]:
        return []

    def __str__(self) -> str:
        return f"{self.alias}:{self.entity_name}"


@dataclass
class NodeEntity(Entity):
    """A node entity in a MATCH pattern."""

    pass


class RelationshipDirection(Enum):
    """Direction of a relationship."""

    FORWARD = auto()  # (a)-[r]->(b)
    BACKWARD = auto()  # (a)<-[r]-(b)
    BOTH = auto()  # (a)-[r]-(b)


@dataclass
class RelationshipEntity(Entity):
    """A relationship entity in a MATCH pattern."""

    direction: RelationshipDirection = RelationshipDirection.BOTH
    left_entity_name: str = ""
    right_entity_name: str = ""
    # Variable-length path support
    min_hops: int | None = None  # None means fixed (not variable length)
    max_hops: int | None = None  # None means unlimited

    @property
    def is_variable_length(self) -> bool:
        """Check if this is a variable-length relationship pattern."""
        return self.min_hops is not None or self.max_hops is not None

    def __str__(self) -> str:
        dir_char = {
            RelationshipDirection.FORWARD: "->",
            RelationshipDirection.BACKWARD: "<-",
            RelationshipDirection.BOTH: "-",
        }[self.direction]
        hops = ""
        if self.is_variable_length:
            if self.min_hops is not None and self.max_hops is not None:
                hops = f"*{self.min_hops}..{self.max_hops}"
            elif self.min_hops is not None:
                hops = f"*{self.min_hops}.."
            elif self.max_hops is not None:
                hops = f"*..{self.max_hops}"
            else:
                hops = "*"
        return f"[{self.alias}:{self.entity_name}{hops}]{dir_char}"


# ==============================================================================
# Clause Nodes
# ==============================================================================


@dataclass
class MatchClause(TreeNode):
    """A MATCH clause."""

    pattern_parts: list[Entity] = field(default_factory=list)
    is_optional: bool = False
    where_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = list(self.pattern_parts)
        if self.where_expression:
            result.append(self.where_expression)
        return result

    def __str__(self) -> str:
        optional = "OPTIONAL " if self.is_optional else ""
        pattern = ", ".join(str(p) for p in self.pattern_parts)
        where_part = f" WHERE {self.where_expression}" if self.where_expression else ""
        return f"{optional}MATCH {pattern}{where_part}"


# ==============================================================================
# Query Nodes
# ==============================================================================


@dataclass
class QueryNode(TreeNode, ABC):
    """Base class for query nodes."""

    pass


@dataclass
class PartialQueryNode(QueryNode):
    """A partial query (single reading/updating clause)."""

    match_clauses: list[MatchClause] = field(default_factory=list)
    where_expression: QueryExpression | None = None
    return_body: list[QueryExpressionWithAlias] = field(default_factory=list)
    order_by: list[SortItem] = field(default_factory=list)
    limit_clause: LimitClause | None = None
    is_distinct: bool = False

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = list(self.match_clauses)
        if self.where_expression:
            result.append(self.where_expression)
        result.extend(self.return_body)
        result.extend(self.order_by)
        if self.limit_clause:
            result.append(self.limit_clause)
        return result

    def __str__(self) -> str:
        parts: list[str] = []
        for match in self.match_clauses:
            parts.append(str(match))
        if self.where_expression:
            parts.append(f"WHERE {self.where_expression}")
        if self.return_body:
            distinct = "DISTINCT " if self.is_distinct else ""
            returns = ", ".join(str(r) for r in self.return_body)
            parts.append(f"RETURN {distinct}{returns}")
        if self.order_by:
            order = ", ".join(str(o) for o in self.order_by)
            parts.append(f"ORDER BY {order}")
        if self.limit_clause:
            parts.append(str(self.limit_clause))
        return " ".join(parts)


@dataclass
class SingleQueryNode(QueryNode):
    """A single complete query."""

    parts: list[PartialQueryNode] = field(default_factory=list)

    @property
    def children(self) -> list[TreeNode]:
        return list(self.parts)

    def __str__(self) -> str:
        return " ".join(str(p) for p in self.parts)


class InfixOperator(Enum):
    """Set operators for combining queries."""

    UNION = auto()
    UNION_ALL = auto()


@dataclass
class InfixQueryNode(QueryNode):
    """A query combining two queries with a set operator."""

    operator: InfixOperator
    left_query: QueryNode
    right_query: QueryNode

    @property
    def children(self) -> list[TreeNode]:
        return [self.left_query, self.right_query]

    def __str__(self) -> str:
        op = "UNION ALL" if self.operator == InfixOperator.UNION_ALL else "UNION"
        return f"({self.left_query}) {op} ({self.right_query})"
