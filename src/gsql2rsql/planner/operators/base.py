"""Base classes for logical operators in the query plan."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TypeVar

from gsql2rsql.common.exceptions import TranspilerInternalErrorException
from gsql2rsql.common.schema import IGraphSchemaProvider
from gsql2rsql.planner.schema import (
    EntityField,
    Schema,
    ValueField,
)

# TypeVar for generic methods (Python 3.11 compatible, replaces PEP 695 syntax)
_TLogicalOp = TypeVar("_TLogicalOp", bound="LogicalOperator")


class IBindable(ABC):
    """Interface for operators that can be bound to a graph schema."""

    @abstractmethod
    def bind(self, graph_definition: IGraphSchemaProvider) -> None:
        """Bind this operator to a graph schema definition."""
        ...


@dataclass
class LogicalOperator(ABC):
    """
    Base class for logical operators in the query plan.

    The logical plan is a DAG (directed acyclic graph) of logical operators.

    Note on public graph structure attributes:
        `graph_in_operators` and `graph_out_operators` are intentionally public
        (not prefixed with `_`) because the SubqueryOptimizer needs direct access
        to rewire the operator graph during optimization passes. This includes:
        - Removing operators from the graph
        - Replacing connections between operators
        - Orphaning flattened operators

        If you need read-only access, prefer using `in_operators` and `out_operators`
        properties. Direct mutation should only be done by optimizer code.
    """

    input_schema: Schema = field(default_factory=Schema)
    output_schema: Schema = field(default_factory=Schema)
    operator_debug_id: int = 0
    # Public for optimizer rewiring - see class docstring
    graph_in_operators: list[LogicalOperator] = field(default_factory=list)
    graph_out_operators: list[LogicalOperator] = field(default_factory=list)

    @property
    @abstractmethod
    def depth(self) -> int:
        """The level of this operator in the plan."""
        ...

    @property
    def in_operators(self) -> list[LogicalOperator]:
        """Upstream operators (read-only access preferred)."""
        return self.graph_in_operators

    @property
    def out_operators(self) -> list[LogicalOperator]:
        """Downstream operators (read-only access preferred)."""
        return self.graph_out_operators

    def add_in_operator(self, op: LogicalOperator) -> None:
        """Add an upstream operator."""
        if op in self.graph_in_operators:
            raise TranspilerInternalErrorException(f"Operator {op} already added")
        self.graph_in_operators.append(op)

    def add_out_operator(self, op: LogicalOperator) -> None:
        """Add a downstream operator."""
        if op in self.graph_out_operators:
            raise TranspilerInternalErrorException(f"Operator {op} already added")
        self.graph_out_operators.append(op)

    def get_all_downstream_operators(
        self, op_type: type[_TLogicalOp]
    ) -> Iterator[_TLogicalOp]:
        """Get all downstream operators of a specific type."""
        if isinstance(self, op_type):
            yield self
        for out_op in self.graph_out_operators:
            yield from out_op.get_all_downstream_operators(op_type)

    def get_all_upstream_operators(
        self, op_type: type[_TLogicalOp]
    ) -> Iterator[_TLogicalOp]:
        """Get all upstream operators of a specific type."""
        if isinstance(self, op_type):
            yield self
        for in_op in self.graph_in_operators:
            yield from in_op.get_all_upstream_operators(op_type)

    def get_input_operator(self) -> LogicalOperator | None:
        """Get the primary input operator (first upstream operator).

        This provides a polymorphic way to access the input operator without
        needing to check if the operator is Unary, Binary, or Start type.
        For Unary operators, returns the single input.
        For Binary operators, returns the left input.
        For Start operators, returns None.
        """
        return self.graph_in_operators[0] if self.graph_in_operators else None

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema."""
        pass

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema."""
        pass

    # =========================================================================
    # Schema Propagation API (new methods for complete schema propagation)
    # See docs/development/schema-propagation.md for design details.
    # =========================================================================

    def get_output_scope(self) -> Schema:
        """Return the authoritative output scope for this operator.

        This is the single source of truth for what columns are available
        downstream. The renderer MUST use this instead of guessing.

        Returns:
            Schema containing all fields available to downstream operators.
        """
        return self.output_schema

    def required_input_symbols(self) -> set[str]:
        """Return symbols required from input to compute output.

        If a required symbol is not in input_schema, propagation should fail.
        Override in subclasses that consume symbols (e.g., Selection, Projection).

        Returns:
            Set of symbol names (field aliases) required from input.
        """
        # Default: no requirements
        return set()

    def introduced_symbols(self) -> set[str]:
        """Return symbols newly created by this operator.

        These are symbols that don't exist in the input but are created
        by this operator (e.g., DataSource introduces entity alias).
        Override in subclasses that create new symbols.

        Returns:
            Set of symbol names (field aliases) introduced by this operator.
        """
        # Default: no new symbols
        return set()

    def dump_scope(self) -> str:
        """Return human-readable dump of the output scope.

        Useful for debugging schema propagation issues.

        Returns:
            Multi-line string describing the output scope.
        """
        lines = [f"=== {self.__class__.__name__} (id={self.operator_debug_id}) ==="]
        lines.append(f"Output Scope ({len(self.output_schema)} fields):")
        for f in self.output_schema:
            if isinstance(f, EntityField):
                props = [ef.field_alias for ef in f.encapsulated_fields]
                lines.append(
                    f"  {f.field_alias}: {f.entity_name} "
                    f"({f.entity_type.name}) props={props}"
                )
            elif isinstance(f, ValueField):
                # Show authoritative structured_type when available
                if f.structured_type is not None:
                    type_info = f"authoritative={f.structured_type.sql_type_name()}"
                else:
                    type_info = f"legacy_type={f.data_type}"
                lines.append(
                    f"  {f.field_alias}: {f.field_name} ({type_info})"
                )
            else:
                lines.append(f"  {f.field_alias}: {type(f).__name__}")
        return "\n".join(lines)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.operator_debug_id})"


@dataclass
class UnaryLogicalOperator(LogicalOperator):
    """Operator with a single input."""

    @property
    def in_operator(self) -> LogicalOperator | None:
        """Get the single input operator."""
        return self.graph_in_operators[0] if self.graph_in_operators else None

    def set_in_operator(self, op: LogicalOperator) -> None:
        """Set the input operator."""
        self.graph_in_operators = [op]
        op.add_out_operator(self)


@dataclass
class BinaryLogicalOperator(LogicalOperator):
    """Operator with two inputs."""

    @property
    def in_operator_left(self) -> LogicalOperator | None:
        """Get the left input operator."""
        return self.graph_in_operators[0] if len(self.graph_in_operators) > 0 else None

    @property
    def in_operator_right(self) -> LogicalOperator | None:
        """Get the right input operator."""
        return self.graph_in_operators[1] if len(self.graph_in_operators) > 1 else None

    def set_in_operators(self, left: LogicalOperator, right: LogicalOperator) -> None:
        """Set both input operators."""
        self.graph_in_operators = [left, right]
        left.add_out_operator(self)
        right.add_out_operator(self)


@dataclass
class StartLogicalOperator(LogicalOperator):
    """Starting operator with no inputs (data source)."""

    @property
    def depth(self) -> int:
        return 0
