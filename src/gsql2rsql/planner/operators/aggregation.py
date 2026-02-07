"""AggregationBoundaryOperator for WITH-aggregation boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field

from gsql2rsql.parser.ast import QueryExpression
from gsql2rsql.planner.operators.base import UnaryLogicalOperator
from gsql2rsql.planner.schema import (
    Field,
    Schema,
    ValueField,
)


@dataclass
class AggregationBoundaryOperator(UnaryLogicalOperator):
    """Operator representing a materialization boundary after aggregation.

    This operator represents a WITH clause that contains aggregation functions.
    It creates a "checkpoint" in the query plan where:
    - The input relation is aggregated according to group_keys and aggregates
    - Only projected columns are visible after this point
    - Subsequent MATCH clauses must join with the aggregated result

    This is semantically equivalent to a SQL CTE/subquery and enforces Cypher's
    variable scoping rules where aggregating WITH creates a new scope.

    Example:
        MATCH (a)-[:R1]->(b)
        WITH a, COUNT(b) AS cnt    -- Creates AggregationBoundaryOperator
        WHERE cnt > 5              -- Having filter (applied after aggregation)
        MATCH (a)-[:R2]->(c)       -- Joins with aggregated result
        RETURN a, cnt, COUNT(c)

    Attributes:
        group_keys: List of (alias, expression) tuples for GROUP BY columns.
            These are the non-aggregated columns from the WITH clause.
        aggregates: List of (alias, aggregation_expression) tuples for aggregations.
            These are the columns containing aggregate functions (COUNT, SUM, etc.)
        having_filter: Optional filter expression applied AFTER aggregation (HAVING).
            Comes from WITH ... WHERE when the filter references aggregated columns.
        order_by: Optional list of (expression, is_descending) for ORDER BY.
        limit: Optional LIMIT value.
        skip: Optional SKIP/OFFSET value.
        cte_name: Name for the CTE when rendered to SQL (auto-generated if not set).
        projected_variables: Set of variable names that are projected through this
            boundary. Used to validate that subsequent MATCHes can reference them.
    """

    group_keys: list[tuple[str, QueryExpression]] = field(default_factory=list)
    aggregates: list[tuple[str, QueryExpression]] = field(default_factory=list)
    having_filter: QueryExpression | None = None
    order_by: list[tuple[QueryExpression, bool]] = field(default_factory=list)
    limit: int | None = None
    skip: int | None = None
    cte_name: str = ""
    projected_variables: set[str] = field(default_factory=set)

    @property
    def depth(self) -> int:
        return (self.in_operator.depth if self.in_operator else 0) + 1

    @property
    def all_projections(self) -> list[tuple[str, QueryExpression]]:
        """Return all projections (group keys + aggregates) in order."""
        return self.group_keys + self.aggregates

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema."""
        if self.in_operator and self.in_operator.output_schema:
            self.input_schema = Schema(self.in_operator.output_schema.fields)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema.

        The output schema contains only the projected columns (group keys + aggregates).
        Entity fields from input are converted to ValueFields with their ID columns.
        """
        fields: list[Field] = []
        for alias, _ in self.all_projections:
            # Create a ValueField for each projected column
            # The actual data type would be inferred from the expression
            fields.append(ValueField(
                field_alias=alias,
                field_name=alias,
                data_type=None,  # Would need type inference
            ))
        self.output_schema = Schema(fields)

    def __str__(self) -> str:
        base = super().__str__()
        group_str = ", ".join(alias for alias, _ in self.group_keys)
        agg_str = ", ".join(alias for alias, _ in self.aggregates)
        having_str = f"\n  Having: {self.having_filter}" if self.having_filter else ""
        return f"{base}\n  GroupBy: [{group_str}]\n  Aggregates: [{agg_str}]{having_str}"
