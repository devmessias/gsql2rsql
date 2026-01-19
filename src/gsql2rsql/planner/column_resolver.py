"""Column resolution pass for the transpiler.

This module provides the ColumnResolver class which performs a full resolution
pass over the logical plan, validating all column references and creating
ResolvedColumnRef objects for use during rendering.

The resolution pass:
1. Builds a symbol table by visiting operators in topological order
2. Resolves all column references in expressions
3. Validates property accesses against entity schemas
4. Provides detailed error messages for invalid references
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from gsql2rsql.common.exceptions import (
    ColumnResolutionError,
    ColumnResolutionErrorContext,
    levenshtein_distance,
)
from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionCaseExpression,
    QueryExpressionFunction,
    QueryExpressionList,
    QueryExpressionListComprehension,
    QueryExpressionListPredicate,
    QueryExpressionMapLiteral,
    QueryExpressionProperty,
    QueryExpressionReduce,
    QueryExpressionValue,
    QueryExpressionWithAlias,
    QueryExpressionExists,
)
from gsql2rsql.planner.column_ref import (
    ColumnRefType,
    ResolvedColumnRef,
    ResolvedExpression,
    ResolvedProjection,
    compute_sql_column_name,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    DataSourceOperator,
    JoinOperator,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
    SetOperator,
    UnwindOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType, Schema, ValueField
from gsql2rsql.planner.symbol_table import (
    SymbolEntry,
    SymbolInfo,
    SymbolTable,
    SymbolType,
)

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan


@dataclass
class ResolutionResult:
    """Result of the column resolution pass.

    Contains the resolved plan and any warnings/info collected during resolution.
    """

    # Resolved expressions per operator (operator_id -> resolved expressions)
    resolved_expressions: dict[int, list[ResolvedExpression]] = field(default_factory=dict)

    # Resolved projections per ProjectionOperator
    resolved_projections: dict[int, list[ResolvedProjection]] = field(default_factory=dict)

    # Final symbol table state
    symbol_table: SymbolTable | None = None

    # Statistics
    total_references_resolved: int = 0
    total_expressions_resolved: int = 0

    # Warnings (non-fatal issues)
    warnings: list[str] = field(default_factory=list)


class ColumnResolver:
    """Resolves and validates all column references in a logical plan.

    The resolver performs a single pass over the logical plan, building a symbol
    table and resolving all column references. It provides detailed error messages
    when resolution fails.

    Usage:
        resolver = ColumnResolver()
        result = resolver.resolve(plan, original_query_text)

        # Access resolved expressions
        for op_id, exprs in result.resolved_expressions.items():
            for expr in exprs:
                for ref in expr.all_refs():
                    print(f"{ref.original_text} -> {ref.sql_column_name}")

    The resolver validates:
    - All variable references exist in scope
    - All property accesses are valid for the entity type
    - Scope boundaries (WITH aggregation) are respected
    - Type compatibility (where determinable)
    """

    def __init__(self) -> None:
        """Initialize the resolver."""
        self._symbol_table: SymbolTable = SymbolTable()
        self._original_query: str = ""
        self._current_operator: LogicalOperator | None = None
        self._current_phase: str = ""
        self._current_part_index: int = 0
        self._result: ResolutionResult = ResolutionResult()

    def resolve(
        self,
        plan: LogicalPlan,
        original_query: str = "",
    ) -> ResolutionResult:
        """Resolve all column references in a logical plan.

        Args:
            plan: The logical plan to resolve
            original_query: The original Cypher query text (for error messages)

        Returns:
            ResolutionResult containing all resolved references

        Raises:
            ColumnResolutionError: If any reference cannot be resolved
        """
        self._symbol_table = SymbolTable()
        self._original_query = original_query
        self._result = ResolutionResult()

        # Phase 1: Build symbol table by visiting operators in topological order
        self._current_phase = "symbol_table_building"
        all_operators = self._get_operators_in_topological_order(plan)

        for op in all_operators:
            self._current_operator = op
            self._build_symbols_for_operator(op)

        # Phase 2: Resolve all expressions
        self._current_phase = "expression_resolution"
        for op in all_operators:
            self._current_operator = op
            self._resolve_operator_expressions(op)

        self._result.symbol_table = self._symbol_table
        return self._result

    def _get_operators_in_topological_order(
        self, plan: LogicalPlan
    ) -> list[LogicalOperator]:
        """Get all operators in topological order (sources first).

        Args:
            plan: The logical plan

        Returns:
            List of operators ordered from sources to terminals
        """
        result: list[LogicalOperator] = []
        visited: set[int] = set()

        def visit(op: LogicalOperator) -> None:
            op_id = id(op)
            if op_id in visited:
                return
            visited.add(op_id)

            # Visit inputs first
            for in_op in op._in_operators:
                visit(in_op)

            result.append(op)

        # Start from terminal operators and work backwards
        for terminal in plan.terminal_operators:
            visit(terminal)

        return result

    def _build_symbols_for_operator(self, op: LogicalOperator) -> None:
        """Build symbol table entries for an operator.

        Different operators introduce symbols differently:
        - DataSourceOperator: Introduces entity symbol
        - ProjectionOperator: May introduce value symbols (aliases)
        - AggregationBoundaryOperator: Clears scope, introduces projected symbols
        - UnwindOperator: Introduces value symbol for unwound variable
        - JoinOperator: Merges schemas (no new symbols)
        - SelectionOperator: No new symbols

        Args:
            op: The operator to process
        """
        if isinstance(op, DataSourceOperator):
            self._build_symbols_for_datasource(op)
        elif isinstance(op, AggregationBoundaryOperator):
            self._build_symbols_for_aggregation_boundary(op)
        elif isinstance(op, UnwindOperator):
            self._build_symbols_for_unwind(op)
        elif isinstance(op, ProjectionOperator):
            self._build_symbols_for_projection(op)
        elif isinstance(op, RecursiveTraversalOperator):
            self._build_symbols_for_recursive_traversal(op)
        # JoinOperator, SelectionOperator, etc. don't introduce new symbols

    def _build_symbols_for_datasource(self, op: DataSourceOperator) -> None:
        """Build symbol for a DataSource operator (entity definition).

        Args:
            op: The DataSourceOperator
        """
        if not op.entity or not op.entity.alias:
            return

        alias = op.entity.alias
        entity_name = op.entity.entity_name or "unknown"

        # Get EntityField from output_schema if available
        entity_field: EntityField | None = None
        properties: list[str] = []

        for fld in op.output_schema:
            if isinstance(fld, EntityField) and fld.field_alias == alias:
                entity_field = fld
                properties = [vf.field_name for vf in fld.encapsulated_fields]
                break

        # Determine symbol type based on entity type
        from gsql2rsql.parser.ast import NodeEntity, RelationshipEntity

        if isinstance(op.entity, RelationshipEntity):
            entity_type_name = "RELATIONSHIP"
        else:
            entity_type_name = "NODE"

        entry = SymbolEntry(
            name=alias,
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=op.operator_debug_id,
            definition_location=f"MATCH ({alias}:{entity_name})",
            scope_level=self._symbol_table.current_level,
            entity_info=entity_field,
            data_type_name=entity_name,
            properties=properties,
        )

        self._symbol_table.define_or_update(alias, entry)

    def _build_symbols_for_aggregation_boundary(
        self, op: AggregationBoundaryOperator
    ) -> None:
        """Build symbols for an aggregation boundary.

        An aggregation boundary clears the current scope and introduces
        only the projected variables.

        Args:
            op: The AggregationBoundaryOperator
        """
        # Clear scope - after aggregation, only projected variables are visible
        self._symbol_table.clear_scope_for_aggregation(
            f"WITH aggregation at operator {op.operator_debug_id}"
        )

        # Add symbols for each projection
        for alias, expr in op.all_projections:
            # Determine if this is an entity reference or computed value
            is_entity = False
            entity_info = None
            data_type_name = None
            properties: list[str] = []

            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name is None:
                    # Bare entity reference like "WITH a, ..."
                    existing = self._symbol_table.lookup(expr.variable_name)
                    if existing and existing.symbol_type == SymbolType.ENTITY:
                        is_entity = True
                        entity_info = existing.entity_info
                        data_type_name = existing.data_type_name
                        properties = existing.properties

            # Check if this is an aggregation result
            is_aggregated = self._expression_contains_aggregation(expr)
            if is_aggregated:
                data_type_name = self._infer_aggregation_type(expr)

            entry = SymbolEntry(
                name=alias,
                symbol_type=SymbolType.ENTITY if is_entity else SymbolType.VALUE,
                definition_operator_id=op.operator_debug_id,
                definition_location=f"WITH (aggregation)",
                scope_level=self._symbol_table.current_level,
                entity_info=entity_info,
                data_type_name=data_type_name,
                properties=properties,
                is_aggregated=is_aggregated,
            )

            self._symbol_table.define_or_update(alias, entry)

    def _build_symbols_for_unwind(self, op: UnwindOperator) -> None:
        """Build symbol for an UNWIND operator.

        Args:
            op: The UnwindOperator
        """
        entry = SymbolEntry(
            name=op.variable_name,
            symbol_type=SymbolType.VALUE,
            definition_operator_id=op.operator_debug_id,
            definition_location=f"UNWIND ... AS {op.variable_name}",
            scope_level=self._symbol_table.current_level,
            data_type_name="ANY",  # Could infer from list element type
        )

        self._symbol_table.define_or_update(op.variable_name, entry)

    def _build_symbols_for_recursive_traversal(
        self, op: RecursiveTraversalOperator
    ) -> None:
        """Build symbols for a RecursiveTraversal operator.

        RecursiveTraversal introduces:
        - path_variable (if specified) - the named path variable
        - Target nodes are handled by their own DataSourceOperators

        Args:
            op: The RecursiveTraversalOperator
        """
        # Add path variable if specified
        if op.path_variable:
            entry = SymbolEntry(
                name=op.path_variable,
                symbol_type=SymbolType.PATH,
                definition_operator_id=op.operator_debug_id,
                definition_location=f"MATCH {op.path_variable} = ...",
                scope_level=self._symbol_table.current_level,
                data_type_name="PATH",  # Path type
            )
            self._symbol_table.define_or_update(op.path_variable, entry)

    def _build_symbols_for_projection(self, op: ProjectionOperator) -> None:
        """Build symbols for a projection operator.

        Projections that rename or compute values create new symbols.

        Args:
            op: The ProjectionOperator
        """
        for alias, expr in op.projections:
            # Check if this creates a new name (alias differs from source)
            creates_new_name = True
            source_entry: SymbolEntry | None = None

            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name is None:
                    # Bare entity reference - doesn't create new name if alias matches
                    if expr.variable_name == alias:
                        creates_new_name = False
                    source_entry = self._symbol_table.lookup(expr.variable_name)

            if creates_new_name:
                # Determine type from expression
                is_aggregated = self._expression_contains_aggregation(expr)
                data_type_name = None

                if is_aggregated:
                    data_type_name = self._infer_aggregation_type(expr)
                elif source_entry:
                    data_type_name = source_entry.data_type_name

                entry = SymbolEntry(
                    name=alias,
                    symbol_type=SymbolType.VALUE,
                    definition_operator_id=op.operator_debug_id,
                    definition_location=f"RETURN/WITH AS {alias}",
                    scope_level=self._symbol_table.current_level,
                    data_type_name=data_type_name,
                    is_aggregated=is_aggregated,
                )

                self._symbol_table.define_or_update(alias, entry)

    def _resolve_operator_expressions(self, op: LogicalOperator) -> None:
        """Resolve all expressions in an operator.

        Args:
            op: The operator to process
        """
        resolved_exprs: list[ResolvedExpression] = []

        if isinstance(op, SelectionOperator):
            if op.filter_expression:
                resolved = self._resolve_expression(op.filter_expression)
                resolved_exprs.append(resolved)

        elif isinstance(op, ProjectionOperator):
            resolved_projections: list[ResolvedProjection] = []
            for alias, expr in op.projections:
                resolved_expr = self._resolve_expression(expr)
                resolved_exprs.append(resolved_expr)

                # Create ResolvedProjection
                is_entity_ref = (
                    isinstance(expr, QueryExpressionProperty)
                    and expr.property_name is None
                )
                entity_id_column = None
                if is_entity_ref:
                    entity_id_column = compute_sql_column_name(
                        expr.variable_name, None
                    )

                resolved_projections.append(ResolvedProjection(
                    alias=alias,
                    expression=resolved_expr,
                    sql_output_name=alias,  # Use user's alias for output
                    is_entity_ref=is_entity_ref,
                    entity_id_column=entity_id_column,
                ))

            self._result.resolved_projections[op.operator_debug_id] = resolved_projections

            # Also resolve filter and having expressions
            if op.filter_expression:
                resolved_exprs.append(self._resolve_expression(op.filter_expression))
            if op.having_expression:
                resolved_exprs.append(self._resolve_expression(op.having_expression))

            # Resolve ORDER BY expressions
            for order_expr, _ in op.order_by:
                resolved_exprs.append(self._resolve_expression(order_expr))

        elif isinstance(op, DataSourceOperator):
            if op.filter_expression:
                resolved = self._resolve_expression(op.filter_expression)
                resolved_exprs.append(resolved)

        elif isinstance(op, AggregationBoundaryOperator):
            for alias, expr in op.all_projections:
                resolved_exprs.append(self._resolve_expression(expr))
            if op.having_filter:
                resolved_exprs.append(self._resolve_expression(op.having_filter))

        elif isinstance(op, UnwindOperator):
            if op.list_expression:
                resolved = self._resolve_expression(op.list_expression)
                resolved_exprs.append(resolved)

        elif isinstance(op, RecursiveTraversalOperator):
            if op.edge_filter:
                resolved = self._resolve_expression(op.edge_filter)
                resolved_exprs.append(resolved)

        if resolved_exprs:
            self._result.resolved_expressions[op.operator_debug_id] = resolved_exprs
            self._result.total_expressions_resolved += len(resolved_exprs)

    def _resolve_expression(self, expr: QueryExpression) -> ResolvedExpression:
        """Resolve all column references in an expression.

        Args:
            expr: The expression to resolve

        Returns:
            ResolvedExpression with all references resolved

        Raises:
            ColumnResolutionError: If any reference cannot be resolved
        """
        resolved = ResolvedExpression(original_expression=expr)
        self._resolve_expression_recursive(expr, resolved)
        return resolved

    def _resolve_expression_recursive(
        self,
        expr: QueryExpression,
        resolved: ResolvedExpression,
    ) -> None:
        """Recursively resolve column references in an expression.

        Args:
            expr: The expression to resolve
            resolved: The ResolvedExpression to populate
        """
        if isinstance(expr, QueryExpressionProperty):
            ref = self._resolve_property_reference(expr)
            key = ref.original_text
            resolved.column_refs[key] = ref
            self._result.total_references_resolved += 1

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression:
                self._resolve_expression_recursive(expr.left_expression, resolved)
            if expr.right_expression:
                self._resolve_expression_recursive(expr.right_expression, resolved)

        elif isinstance(expr, QueryExpressionFunction):
            for param in expr.parameters or []:
                self._resolve_expression_recursive(param, resolved)

        elif isinstance(expr, QueryExpressionAggregationFunction):
            if expr.inner_expression:
                self._resolve_expression_recursive(expr.inner_expression, resolved)

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression:
                self._resolve_expression_recursive(expr.test_expression, resolved)
            for when_expr, then_expr in expr.alternatives or []:
                self._resolve_expression_recursive(when_expr, resolved)
                self._resolve_expression_recursive(then_expr, resolved)
            if expr.else_expression:
                self._resolve_expression_recursive(expr.else_expression, resolved)

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                self._resolve_expression_recursive(item, resolved)

        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression:
                self._resolve_expression_recursive(expr.list_expression, resolved)
            if expr.filter_expression:
                self._resolve_expression_recursive(expr.filter_expression, resolved)

        elif isinstance(expr, QueryExpressionListComprehension):
            # Resolve the list expression first (e.g., nodes(path))
            if expr.list_expression:
                self._resolve_expression_recursive(expr.list_expression, resolved)

            # Add the loop variable as a temporary local symbol
            # e.g., in [n IN nodes(path) | n.id], 'n' is a local binding
            loop_var = expr.variable_name
            temp_entry = SymbolEntry(
                name=loop_var,
                symbol_type=SymbolType.VALUE,
                definition_operator_id=0,
                definition_location=f"[{loop_var} IN ...]",
                scope_level=self._symbol_table.current_level,
                data_type_name="ANY",  # Element type of list
            )
            # Save existing entry if any (for nested comprehensions)
            existing_entry = self._symbol_table.lookup(loop_var)
            self._symbol_table.define_or_update(loop_var, temp_entry)

            # Now resolve filter and map expressions with the loop variable in scope
            if expr.filter_expression:
                self._resolve_expression_recursive(expr.filter_expression, resolved)
            if expr.map_expression:
                self._resolve_expression_recursive(expr.map_expression, resolved)

            # Restore the original entry or remove the temporary one
            if existing_entry:
                self._symbol_table.define_or_update(loop_var, existing_entry)
            else:
                # Remove the temporary entry - not strictly necessary but cleaner
                # The symbol table doesn't have a remove method, so we leave it
                pass

        elif isinstance(expr, QueryExpressionReduce):
            if expr.initial_expression:
                self._resolve_expression_recursive(expr.initial_expression, resolved)
            if expr.list_expression:
                self._resolve_expression_recursive(expr.list_expression, resolved)
            if expr.accumulator_expression:
                self._resolve_expression_recursive(expr.accumulator_expression, resolved)

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression:
                self._resolve_expression_recursive(expr.inner_expression, resolved)

        elif isinstance(expr, QueryExpressionMapLiteral):
            for key, value in expr.entries or []:
                self._resolve_expression_recursive(value, resolved)

        elif isinstance(expr, QueryExpressionExists):
            # EXISTS subqueries have their own scope - skip for now
            # TODO: Handle EXISTS subquery resolution with nested scope
            pass

        elif isinstance(expr, QueryExpressionValue):
            # Literal values don't need resolution
            pass

    def _resolve_property_reference(
        self, expr: QueryExpressionProperty
    ) -> ResolvedColumnRef:
        """Resolve a property reference (variable or variable.property).

        Args:
            expr: The property expression

        Returns:
            ResolvedColumnRef for this reference

        Raises:
            ColumnResolutionError: If the reference cannot be resolved
        """
        variable = expr.variable_name
        property_name = expr.property_name

        # Look up variable in symbol table
        entry = self._symbol_table.lookup(variable)

        if entry is None:
            self._raise_undefined_variable_error(expr)

        # Determine reference type and validate
        if property_name is None:
            # Bare entity reference (e.g., "p" in RETURN p)
            ref_type = ColumnRefType.ENTITY_ID
            sql_name = compute_sql_column_name(variable, None)

            # If this is a value symbol (not entity), use VALUE_ALIAS type
            if entry.symbol_type == SymbolType.VALUE:
                ref_type = ColumnRefType.VALUE_ALIAS
                sql_name = variable  # Use the alias directly

        else:
            # Property access (e.g., "p.name")
            ref_type = ColumnRefType.ENTITY_PROPERTY

            # Validate property exists on entity
            if entry.symbol_type == SymbolType.ENTITY:
                if entry.properties and property_name not in entry.properties:
                    # Property doesn't exist - but only warn if we have property info
                    # Some entities might not have full schema info
                    if entry.properties:
                        self._raise_invalid_property_error(expr, entry)

            sql_name = compute_sql_column_name(variable, property_name)

        return ResolvedColumnRef(
            original_variable=variable,
            original_property=property_name,
            ref_type=ref_type,
            source_operator_id=entry.definition_operator_id,
            sql_column_name=sql_name,
            data_type_name=entry.data_type_name,
            derivation=f"from {entry.definition_location}",
        )

    def _raise_undefined_variable_error(
        self, expr: QueryExpressionProperty
    ) -> None:
        """Raise a detailed error for an undefined variable.

        Args:
            expr: The expression with the undefined variable

        Raises:
            ColumnResolutionError: Always raises with full context
        """
        variable = expr.variable_name

        # Compute suggestions
        suggestions = self._compute_suggestions(variable)

        # Get out-of-scope symbols that might be relevant
        out_of_scope = self._symbol_table.get_out_of_scope_symbols()

        # Build hints
        hints = self._build_hints_for_undefined_variable(variable, out_of_scope)

        context = ColumnResolutionErrorContext(
            error_type="UndefinedVariable",
            message=f"Variable '{variable}' is not defined",
            query_text=self._original_query,
            available_symbols=self._symbol_table.get_available_symbols(),
            out_of_scope_symbols=out_of_scope,
            suggestions=suggestions,
            hints=hints,
            operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
            operator_type=type(self._current_operator).__name__ if self._current_operator else "",
            resolution_phase=self._current_phase,
            symbol_table_dump=self._symbol_table.dump(),
        )

        raise ColumnResolutionError(
            f"Variable '{variable}' is not defined",
            context=context,
        )

    def _raise_invalid_property_error(
        self, expr: QueryExpressionProperty, entry: SymbolEntry
    ) -> None:
        """Raise a detailed error for an invalid property access.

        Args:
            expr: The expression with invalid property
            entry: The symbol entry for the entity

        Raises:
            ColumnResolutionError: Always raises with full context
        """
        variable = expr.variable_name
        property_name = expr.property_name

        # Compute property suggestions
        suggestions = []
        if entry.properties:
            for prop in entry.properties:
                dist = levenshtein_distance(property_name, prop)
                if dist <= 3:
                    suggestions.append(
                        f"Did you mean '{variable}.{prop}'? "
                        f"({dist} character{'s' if dist > 1 else ''} difference)"
                    )

        # Build hints
        hints = [
            f"Entity '{variable}' is of type '{entry.data_type_name}'.\n"
            f"Available properties: {', '.join(entry.properties)}"
        ]

        context = ColumnResolutionErrorContext(
            error_type="InvalidPropertyAccess",
            message=f"Entity '{variable}' has no property '{property_name}'",
            query_text=self._original_query,
            available_symbols=self._symbol_table.get_available_symbols(),
            suggestions=suggestions,
            hints=hints,
            operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
            operator_type=type(self._current_operator).__name__ if self._current_operator else "",
            resolution_phase=self._current_phase,
            symbol_table_dump=self._symbol_table.dump(),
        )

        raise ColumnResolutionError(
            f"Entity '{variable}' has no property '{property_name}'",
            context=context,
        )

    def _compute_suggestions(self, target: str) -> list[str]:
        """Compute 'did you mean' suggestions for a variable name.

        Args:
            target: The variable name that wasn't found

        Returns:
            List of suggestion strings
        """
        suggestions = []
        all_names = self._symbol_table.all_names()

        # Score by edit distance
        scored = [
            (name, levenshtein_distance(target, name))
            for name in all_names
        ]
        scored.sort(key=lambda x: x[1])

        for name, dist in scored[:3]:
            if dist <= 3:
                suggestions.append(
                    f"Did you mean '{name}'? "
                    f"({dist} character{'s' if dist > 1 else ''} difference)"
                )

        # Also suggest out-of-scope variables
        for sym_info, reason in self._symbol_table.get_out_of_scope_symbols():
            if sym_info.name == target:
                suggestions.append(
                    f"Variable '{target}' exists but is out of scope: {reason}"
                )

        return suggestions

    def _build_hints_for_undefined_variable(
        self,
        variable: str,
        out_of_scope: list[tuple[SymbolInfo, str]],
    ) -> list[str]:
        """Build contextual hints for an undefined variable error.

        Args:
            variable: The undefined variable name
            out_of_scope: List of out-of-scope symbols with reasons

        Returns:
            List of hint strings
        """
        hints = []

        # Check if variable is out of scope due to aggregation
        for sym_info, reason in out_of_scope:
            if sym_info.name == variable and "aggregation" in reason.lower():
                hints.append(
                    "After a WITH clause containing aggregation (like COUNT, SUM, etc.),\n"
                    "only variables explicitly listed in the WITH clause are accessible.\n\n"
                    f"To keep '{variable}' available, project it:\n"
                    f"  WITH {variable}, COUNT(...) AS count_result\n\n"
                    f"Or, if you only need '{variable}' for aggregation, it's working correctly -\n"
                    f"'{variable}' is aggregated and no longer available as an entity."
                )
                break

        if not hints:
            hints.append(
                f"Make sure '{variable}' is defined in a MATCH clause before use.\n"
                "Variables must be defined before they can be referenced in WHERE, "
                "WITH, or RETURN clauses."
            )

        return hints

    def _expression_contains_aggregation(self, expr: QueryExpression) -> bool:
        """Check if an expression contains aggregation functions.

        Args:
            expr: The expression to check

        Returns:
            True if the expression contains aggregation
        """
        if isinstance(expr, QueryExpressionAggregationFunction):
            return True

        if isinstance(expr, QueryExpressionBinary):
            left_has = self._expression_contains_aggregation(expr.left_expression) if expr.left_expression else False
            right_has = self._expression_contains_aggregation(expr.right_expression) if expr.right_expression else False
            return left_has or right_has

        if isinstance(expr, QueryExpressionFunction):
            for param in expr.parameters or []:
                if self._expression_contains_aggregation(param):
                    return True

        return False

    def _infer_aggregation_type(self, expr: QueryExpression) -> str:
        """Infer the result type of an aggregation expression.

        Args:
            expr: The aggregation expression

        Returns:
            Type name string (e.g., "INTEGER", "DOUBLE", "ARRAY")
        """
        if isinstance(expr, QueryExpressionAggregationFunction):
            agg_name = expr.aggregation_function.name if expr.aggregation_function else ""
            if agg_name in ("COUNT",):
                return "INTEGER"
            if agg_name in ("SUM", "AVG"):
                return "DOUBLE"
            if agg_name in ("COLLECT",):
                return "ARRAY"
            if agg_name in ("MIN", "MAX"):
                return "ANY"  # Depends on input type
            return "ANY"

        return "ANY"


def resolve_plan(
    plan: LogicalPlan,
    original_query: str = "",
) -> ResolutionResult:
    """Convenience function to resolve a logical plan.

    Args:
        plan: The logical plan to resolve
        original_query: The original Cypher query text

    Returns:
        ResolutionResult containing all resolved references
    """
    resolver = ColumnResolver()
    return resolver.resolve(plan, original_query)
