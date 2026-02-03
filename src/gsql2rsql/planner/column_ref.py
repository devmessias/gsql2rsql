"""Resolved column references for the transpiler.

This module provides types for representing fully-resolved column references.
After the resolution pass, every column reference in the query is resolved
to a ResolvedColumnRef that contains all information needed for rendering.

Key types:
- ResolvedColumnRef: A fully resolved reference to a column
- ResolvedExpression: Wrapper for expressions with resolved references
- ColumnRefType: Classification of column reference types
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gsql2rsql.parser.ast import QueryExpression


class ColumnRefType(Enum):
    """Classification of column reference types."""

    ENTITY_ID = auto()        # Reference to entity's ID column (e.g., p in RETURN p)
    ENTITY_PROPERTY = auto()  # Reference to entity property (e.g., p.name)
    VALUE_ALIAS = auto()      # Reference to a value alias (e.g., friends after WITH)
    STRUCT_FIELD = auto()     # Reference to struct field (e.g., r.src after UNWIND e AS r)
    COMPUTED = auto()         # Computed expression result
    LITERAL = auto()          # Literal value (not a reference)


@dataclass
class ResolvedColumnRef:
    """A fully resolved column reference.

    This represents a column reference that has been validated and resolved
    against the symbol table. It contains all information needed to render
    the reference in SQL.

    Attributes:
        original_variable: The variable name as written (e.g., "p")
        original_property: The property name if any (e.g., "name")
        ref_type: Type of reference (entity ID, property, value alias, etc.)
        source_operator_id: ID of the operator that provides this column
        sql_column_name: The SQL column name to use (e.g., "_gsql2rsql_p_name")
        sql_table_alias: The table/subquery alias if known (e.g., "_gsql2rsql_left")
        data_type: Python type of the value (int, str, bool, etc.)
        data_type_name: Human-readable type name (e.g., "STRING", "INTEGER")
        is_nullable: Whether the column can be NULL
        derivation: Human-readable explanation of where this comes from

    Example:
        For `p.name` in:
            MATCH (p:Person) RETURN p.name

        ResolvedColumnRef(
            original_variable="p",
            original_property="name",
            ref_type=ColumnRefType.ENTITY_PROPERTY,
            source_operator_id=1,
            sql_column_name="_gsql2rsql_p_name",
            sql_table_alias=None,
            data_type=str,
            data_type_name="STRING",
            is_nullable=True,
            derivation="p.name from DataSourceOperator(Person)"
        )
    """

    # Original reference
    original_variable: str
    original_property: str | None = None

    # Resolution result
    ref_type: ColumnRefType = ColumnRefType.ENTITY_PROPERTY
    source_operator_id: int = 0
    sql_column_name: str = ""
    sql_table_alias: str | None = None
    is_entity_return: bool = False  # True if this is a bare entity return (RETURN p vs RETURN p.name)

    # Type information
    data_type: type[Any] | None = None
    data_type_name: str | None = None
    is_nullable: bool = True

    # Debugging
    derivation: str = ""

    @property
    def full_sql_reference(self) -> str:
        """Get the full SQL reference including table alias if present.

        Returns:
            SQL reference like "alias.column" or just "column"
        """
        if self.sql_table_alias:
            return f"{self.sql_table_alias}.{self.sql_column_name}"
        return self.sql_column_name

    @property
    def original_text(self) -> str:
        """Get the original Cypher text representation.

        Returns:
            Original reference like "p.name" or "p"
        """
        if self.original_property:
            return f"{self.original_variable}.{self.original_property}"
        return self.original_variable

    def __str__(self) -> str:
        """Human-readable representation."""
        return f"{self.original_text} -> {self.full_sql_reference} ({self.ref_type.name})"


@dataclass
class ResolvedExpression:
    """An expression with all column references resolved.

    This wraps a QueryExpression and provides access to all resolved
    column references within it. Used to pass resolved expressions
    through the rendering pipeline.

    Attributes:
        original_expression: The original AST expression
        column_refs: Map from original reference text to ResolvedColumnRef
        result_type: The computed result type of the expression
        result_sql_name: SQL column name for expression result (if aliased)
    """

    original_expression: QueryExpression
    column_refs: dict[str, ResolvedColumnRef] = field(default_factory=dict)
    result_type: type[Any] | None = None
    result_sql_name: str | None = None

    def get_ref(self, variable: str, property_name: str | None = None) -> ResolvedColumnRef | None:
        """Get the resolved reference for a variable/property.

        Args:
            variable: Variable name (e.g., "p")
            property_name: Optional property name (e.g., "name")

        Returns:
            ResolvedColumnRef if found, None otherwise
        """
        key = f"{variable}.{property_name}" if property_name else variable
        return self.column_refs.get(key)

    def all_refs(self) -> list[ResolvedColumnRef]:
        """Get all resolved column references in this expression.

        Returns:
            List of all ResolvedColumnRef objects
        """
        return list(self.column_refs.values())

    def __str__(self) -> str:
        """Human-readable representation."""
        refs = ", ".join(str(ref) for ref in self.column_refs.values())
        return f"ResolvedExpression({refs})"


@dataclass
class ResolvedProjection:
    """A resolved projection (alias, expression) pair.

    Used in ProjectionOperator to represent a single output column.

    Attributes:
        alias: Output column alias (e.g., "name")
        expression: The resolved expression
        sql_output_name: SQL column name in output (e.g., "_gsql2rsql_name" or just "name")
        is_entity_ref: True if this projects an entire entity (not a property)
        entity_id_column: For entity refs, the ID column name
    """

    alias: str
    expression: ResolvedExpression
    sql_output_name: str = ""
    is_entity_ref: bool = False
    entity_id_column: str | None = None

    def __str__(self) -> str:
        """Human-readable representation."""
        return f"{self.alias}: {self.expression}"


def compute_sql_column_name(
    variable: str,
    property_name: str | None = None,
    prefix: str = "_gsql2rsql_"
) -> str:
    """Compute the deterministic SQL column name for a reference.

    Uses the _gsql2rsql_ prefix to avoid collisions with user identifiers.

    Args:
        variable: Variable name (e.g., "p")
        property_name: Optional property name (e.g., "name")
        prefix: Prefix to use (default: "_gsql2rsql_")

    Returns:
        SQL column name (e.g., "_gsql2rsql_p_name")
    """
    # Clean variable name (remove non-alphanumeric except underscore)
    clean_var = "".join(c if c.isalnum() or c == "_" else "" for c in variable)

    if property_name:
        clean_prop = "".join(c if c.isalnum() or c == "_" else "" for c in property_name)
        return f"{prefix}{clean_var}_{clean_prop}"
    else:
        # Entity reference (ID)
        return f"{prefix}{clean_var}_id"


def compute_sql_table_alias(
    position: str,
    counter: int | None = None,
    prefix: str = "_gsql2rsql_"
) -> str:
    """Compute a deterministic SQL table/subquery alias.

    Args:
        position: Position indicator (e.g., "left", "right", "sq")
        counter: Optional counter for uniqueness
        prefix: Prefix to use (default: "_gsql2rsql_")

    Returns:
        SQL table alias (e.g., "_gsql2rsql_left", "_gsql2rsql_sq_1")
    """
    if counter is not None:
        return f"{prefix}{position}_{counter}"
    return f"{prefix}{position}"


def compute_cte_name(
    purpose: str,
    counter: int,
    prefix: str = "_gsql2rsql_"
) -> str:
    """Compute a deterministic CTE name.

    Args:
        purpose: Purpose of the CTE (e.g., "agg", "recursive")
        counter: Counter for uniqueness
        prefix: Prefix to use (default: "_gsql2rsql_")

    Returns:
        CTE name (e.g., "_gsql2rsql_agg_1")
    """
    return f"{prefix}{purpose}_{counter}"
