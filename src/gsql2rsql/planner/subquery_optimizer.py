"""Subquery Flattening Optimizer for Logical Plan.

This module optimizes the logical plan by merging consecutive operators that would
otherwise generate unnecessary nested subqueries in the rendered SQL.

================================================================================
CONSERVATIVE FLATTENING APPROACH - READ THIS CAREFULLY
================================================================================

This optimizer uses a CONSERVATIVE approach. We ONLY flatten patterns that are
100% GUARANTEED to produce semantically equivalent SQL. When in doubt, we do NOT
flatten.

WHY CONSERVATIVE?
-----------------
1. Databricks SQL optimizer already flattens simple cases internally
2. Incorrect flattening causes SILENT bugs - wrong results, not errors
3. Users must be able to trust that generated SQL is semantically correct
4. Readability improvement is secondary to correctness

================================================================================
WHAT WE DO FLATTEN (100% SAFE)
================================================================================

✅ RULE 1: Selection → Projection
---------------------------------
Cypher:
    MATCH (p:Person)
    WHERE p.age > 30        <-- SelectionOperator
    RETURN p.name           <-- ProjectionOperator

SQL BEFORE (not flattened):
    SELECT __p_name AS name
    FROM (
        SELECT * FROM (SELECT ...) AS _filter
        WHERE __p_age > 30
    ) AS _proj

SQL AFTER (flattened):
    SELECT __p_name AS name
    FROM (SELECT ...) AS _proj
    WHERE __p_age > 30

WHY SAFE: WHERE clause position doesn't change semantics when there's no
aggregation boundary. The filter applies to the same rows either way.


✅ RULE 2: Selection → Selection
---------------------------------
Cypher (hypothetical - created by complex patterns):
    -- Two consecutive WHERE clauses (rare in practice)

SQL BEFORE (not flattened):
    SELECT * FROM (SELECT * FROM T WHERE A) AS _filter WHERE B

SQL AFTER (flattened):
    SELECT * FROM T WHERE (A) AND (B)

WHY SAFE: WHERE A followed by WHERE B is mathematically equivalent to
WHERE (A AND B). Pure boolean logic - no semantic edge cases.


================================================================================
WHAT WE DO NOT FLATTEN (POTENTIAL SEMANTIC CHANGES)
================================================================================

❌ Projection → Projection
--------------------------
Cypher:
    MATCH (p:Person)
    WITH p.age * 2 AS double_age    <-- ProjectionOperator (alias defined)
    RETURN double_age + 1           <-- ProjectionOperator (references alias)

SQL (NOT flattened - correct):
    SELECT double_age + 1
    FROM (
        SELECT __p_age * 2 AS double_age
        FROM ...
    ) AS _proj

HYPOTHETICAL flattened (WRONG):
    SELECT (__p_age * 2) + 1 AS result  -- Would need to inline the expression
    FROM ...

RISKS IF WE FLATTENED:
1. Column alias conflicts - outer query references alias defined in inner
2. Expression duplication - same expression computed multiple times
3. Side effects - if expressions had side effects (unlikely in SQL)

TODO: Could implement Projection → Projection flattening with these checks:
- Verify outer projections don't reference aliases defined in inner
- Verify no DISTINCT, LIMIT, OFFSET in inner projection
- Inline expressions only when they're simple column references


❌ Anything with LIMIT/OFFSET in inner query
--------------------------------------------
Cypher:
    MATCH (p:Person)
    WITH p ORDER BY p.age LIMIT 10  <-- ProjectionOperator with LIMIT
    WHERE p.active = true           <-- SelectionOperator
    RETURN p.name

SQL (NOT flattened - correct):
    SELECT __p_name AS name
    FROM (
        SELECT * FROM (
            SELECT ... ORDER BY __p_age LIMIT 10
        ) AS _proj
        WHERE __p_active = true
    ) AS _filter

HYPOTHETICAL flattened (WRONG):
    SELECT __p_name AS name
    FROM (SELECT ...) AS _proj
    WHERE __p_active = true
    ORDER BY __p_age LIMIT 10  -- WRONG! WHERE applies BEFORE LIMIT now!

SEMANTIC DIFFERENCE:
- Correct: Take top 10 by age, THEN filter by active
- Wrong: Filter by active, THEN take top 10 by age
- Result: Completely different rows returned!


❌ DISTINCT in inner query
--------------------------
Cypher:
    MATCH (p:Person)-[:KNOWS]->(f:Person)
    WITH DISTINCT p                  <-- ProjectionOperator with DISTINCT
    RETURN COUNT(*)                  <-- Aggregation

SQL (NOT flattened - correct):
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT __p_id, __p_name, ...
        FROM ...
    ) AS _proj

HYPOTHETICAL flattened (WRONG):
    SELECT COUNT(DISTINCT __p_id)  -- Different semantics!
    FROM ...

SEMANTIC DIFFERENCE:
- Correct: Count unique persons (after deduplication)
- Wrong: COUNT DISTINCT on one column only
- If Person has multiple fields, results differ!


❌ Window functions
-------------------
Cypher (hypothetical):
    MATCH (p:Person)
    WITH p, ROW_NUMBER() OVER (ORDER BY p.age) AS rn
    WHERE rn <= 10
    RETURN p.name

RISKS IF FLATTENED:
- Window function scope changes
- Partitioning boundaries affected
- Results could be completely different


================================================================================
EXAMPLES OF BUGS FROM EAGER (NON-CONSERVATIVE) FLATTENING
================================================================================

BUG EXAMPLE 1: Lost rows due to LIMIT reordering
------------------------------------------------
Query: "Get names of top 10 oldest active people"

Cypher:
    MATCH (p:Person)
    WITH p ORDER BY p.age DESC LIMIT 10
    WHERE p.active = true
    RETURN p.name

Expected result (correct): Filter AFTER limit
    1. Sort all people by age DESC
    2. Take top 10
    3. Filter those 10 for active=true
    4. Return names (could be 0-10 rows)

Buggy result (if flattened wrong): Filter BEFORE limit
    1. Filter all people for active=true
    2. Sort by age DESC
    3. Take top 10
    4. Return names (always 10 rows if enough active people)

Impact: User gets WRONG DATA with no error message!


BUG EXAMPLE 2: Wrong count due to DISTINCT flattening
-----------------------------------------------------
Query: "Count unique customers who made purchases"

Cypher:
    MATCH (c:Customer)-[:PURCHASED]->(p:Product)
    WITH DISTINCT c
    RETURN COUNT(*) AS unique_customers

If customer C1 bought 5 products:
- Correct (with DISTINCT subquery): COUNT = 1
- Wrong (if flattened): Could count 5 times!


BUG EXAMPLE 3: Alias resolution failure
---------------------------------------
Query: "Calculate derived value and use it"

Cypher:
    MATCH (p:Person)
    WITH p.salary * 0.3 AS tax
    RETURN tax * 12 AS annual_tax

If flattened incorrectly:
- Outer query references 'tax' but it's not defined at that level
- Could cause runtime SQL error or wrong column reference


================================================================================
TODO: FUTURE OPTIMIZATIONS (NON-CONSERVATIVE, REQUIRES CAREFUL ANALYSIS)
================================================================================

TODO: Projection → Projection flattening
    - Safe ONLY if outer projections are simple column references
    - Must verify no alias conflicts
    - Must verify no DISTINCT, LIMIT, OFFSET in inner
    - Implementation complexity: HIGH
    - Benefit: Moderate (reduces 1 subquery level)

TODO: Selection → Join flattening
    - Push WHERE conditions into JOIN ON clauses
    - Safe for INNER JOIN, risky for OUTER JOIN
    - Could improve query performance
    - Implementation complexity: MEDIUM
    - Benefit: Moderate (better join optimization)

TODO: Recursive CTE flattening
    - Merge post-CTE filters into CTE itself
    - Already partially done with predicate pushdown
    - Full implementation is complex
    - Implementation complexity: HIGH
    - Benefit: High (reduces data processed in recursion)


================================================================================
IMPLEMENTATION NOTES
================================================================================

SINGLE-PASS BOTTOM-UP TRAVERSAL
-------------------------------
We visit operators from leaves to root. This ensures that when we try to
flatten an operator, its children are already in their final form.

Example:
    DataSource → Selection → Selection → Projection
    Visit order: DataSource, Selection1, Selection2, Projection

    At Selection2: Can merge with Selection1 → Selection(A AND B)
    At Projection: Can merge with Selection(A AND B) → Projection with filter

This handles chained patterns in a single pass.


MULTI-PASS NOT NEEDED
---------------------
With bottom-up traversal, we handle chains like Selection → Selection → Projection
correctly in one pass. Multi-pass would only help for patterns we don't support
(like Projection → Projection).


================================================================================
USAGE
================================================================================

    from gsql2rsql.planner.subquery_optimizer import SubqueryFlatteningOptimizer

    # After creating logical plan
    plan = LogicalPlan.process_query_tree(ast, graph_def)

    # Apply optimization (enabled by default)
    optimizer = SubqueryFlatteningOptimizer(enabled=True)
    optimizer.optimize(plan)

    # Check what was flattened
    print(optimizer.stats)  # FlatteningStats(sel→proj=1, sel→sel=0, ...)

    # Render optimized plan
    sql = renderer.render_plan(plan)

    # To disable optimization (for debugging):
    optimizer = SubqueryFlatteningOptimizer(enabled=False)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import QueryExpression, QueryExpressionBinary
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
)

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan


@dataclass
class FlatteningStats:
    """Statistics about flattening operations performed."""

    selection_into_projection: int = 0
    selection_into_selection: int = 0
    total_operators_before: int = 0
    total_operators_after: int = 0

    def __str__(self) -> str:
        return (
            f"FlatteningStats("
            f"sel→proj={self.selection_into_projection}, "
            f"sel→sel={self.selection_into_selection}, "
            f"operators: {self.total_operators_before} → {self.total_operators_after})"
        )


class SubqueryFlatteningOptimizer:
    """Optimizes logical plan by merging operators to reduce subquery nesting.

    This optimizer implements CONSERVATIVE flattening - only patterns that are
    100% semantically equivalent are merged. See module docstring for trade-offs.

    Attributes:
        enabled: Whether optimization is active. Set to False to bypass.
        stats: Statistics about operations performed (for debugging/testing).
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the optimizer.

        Args:
            enabled: If False, optimize() becomes a no-op. Useful for A/B testing
                    or debugging SQL generation issues.
        """
        self.enabled = enabled
        self.stats = FlatteningStats()

    def optimize(self, plan: LogicalPlan) -> None:
        """Apply subquery flattening optimizations to the logical plan.

        Modifies the plan IN-PLACE. The plan's operator graph is rewired
        to eliminate unnecessary intermediate operators.

        Args:
            plan: The logical plan to optimize. Modified in-place.
        """
        if not self.enabled:
            return

        # Reset stats
        self.stats = FlatteningStats()

        # Count operators before
        self.stats.total_operators_before = self._count_operators(plan)

        # Process each terminal operator's subtree
        for terminal_op in plan.terminal_operators:
            self._optimize_subtree(terminal_op)

        # Count operators after
        self.stats.total_operators_after = self._count_operators(plan)

    def _count_operators(self, plan: LogicalPlan) -> int:
        """Count total operators in the plan."""
        visited: set[int] = set()
        count = 0
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                if id(op) not in visited:
                    visited.add(id(op))
                    count += 1
        return count

    def _optimize_subtree(self, op: LogicalOperator) -> None:
        """Recursively optimize a subtree rooted at the given operator.

        Uses bottom-up traversal: children are optimized before parents.
        This ensures that when we check if an operator can be flattened,
        its children are already in their final optimized form.
        """
        # First, recursively optimize children
        for in_op in list(op.in_operators):  # Copy list since we may modify it
            self._optimize_subtree(in_op)

        # Then try to flatten this operator with its input
        self._try_flatten(op)

    def _try_flatten(self, op: LogicalOperator) -> None:
        """Try to flatten this operator with its input operator.

        Dispatches to specific flattening methods based on operator types.
        """
        if isinstance(op, ProjectionOperator):
            self._try_flatten_into_projection(op)
        elif isinstance(op, SelectionOperator):
            self._try_flatten_into_selection(op)

    def _try_flatten_into_projection(self, proj_op: ProjectionOperator) -> None:
        """Try to flatten the input operator into this ProjectionOperator.

        Currently handles:
        - SelectionOperator → ProjectionOperator
        """
        in_op = proj_op.in_operator
        if in_op is None:
            return

        # Rule 1: Selection → Projection
        if isinstance(in_op, SelectionOperator):
            if self._can_flatten_selection_into_projection(in_op, proj_op):
                self._merge_selection_into_projection(in_op, proj_op)

    def _can_flatten_selection_into_projection(
        self,
        selection: SelectionOperator,
        projection: ProjectionOperator,
    ) -> bool:
        """Check if a SelectionOperator can be safely merged into a ProjectionOperator.

        CONSERVATIVE RULES - only flatten when 100% safe:

        1. Selection must have a filter_expression (otherwise nothing to merge)
        2. Projection must not already have a filter_expression (avoid complexity)
        3. Selection's input must not be another Selection (handle one level at a time)

        We DO NOT check for:
        - Column alias conflicts: The filter uses the same column references
          as would be available in the non-flattened version
        - Aggregation interactions: WHERE is always applied before GROUP BY,
          so moving it into Projection (which handles GROUP BY) is safe

        Returns:
            True if flattening is safe and beneficial.
        """
        # Must have something to merge
        if selection.filter_expression is None:
            return False

        # Don't overwrite existing filter (keep it simple)
        if projection.filter_expression is not None:
            return False

        # Selection must have an input to connect to
        if selection.in_operator is None:
            return False

        return True

    def _merge_selection_into_projection(
        self,
        selection: SelectionOperator,
        projection: ProjectionOperator,
    ) -> None:
        """Merge a SelectionOperator into a ProjectionOperator.

        This operation:
        1. Moves filter_expression from Selection to Projection
        2. Bypasses the Selection by connecting Projection directly to Selection's input
        3. Updates operator graph references

        BEFORE:
            Projection._in_operators = [Selection]
            Selection._out_operators = [Projection]
            Selection._in_operators = [SomeOp]
            SomeOp._out_operators = [Selection]

        AFTER:
            Projection._in_operators = [SomeOp]
            Projection.filter_expression = Selection.filter_expression
            SomeOp._out_operators = [Projection]  # Selection removed
            Selection is orphaned (no references)
        """
        # Get the operator that was feeding into Selection
        selection_input = selection.in_operator
        if selection_input is None:
            return

        # Move the filter expression
        projection.filter_expression = selection.filter_expression

        # Rewire the graph:
        # 1. Remove Selection from its input's out_operators
        if selection in selection_input._out_operators:
            selection_input._out_operators.remove(selection)

        # 2. Add Projection to input's out_operators
        if projection not in selection_input._out_operators:
            selection_input._out_operators.append(projection)

        # 3. Update Projection's in_operators to point to Selection's input
        projection._in_operators = [selection_input]

        # 4. Clear Selection's references (orphan it)
        selection._in_operators = []
        selection._out_operators = []

        # Update stats
        self.stats.selection_into_projection += 1

    # =========================================================================
    # Rule 2: Selection → Selection
    # =========================================================================

    def _try_flatten_into_selection(self, outer_sel: SelectionOperator) -> None:
        """Try to flatten the input Selection into this SelectionOperator.

        Handles: SelectionOperator → SelectionOperator (AND filters together)
        """
        in_op = outer_sel.in_operator
        if in_op is None:
            return

        # Rule 2: Selection → Selection
        if isinstance(in_op, SelectionOperator):
            if self._can_flatten_selection_into_selection(in_op, outer_sel):
                self._merge_selection_into_selection(in_op, outer_sel)

    def _can_flatten_selection_into_selection(
        self,
        inner_sel: SelectionOperator,
        outer_sel: SelectionOperator,
    ) -> bool:
        """Check if two consecutive SelectionOperators can be merged.

        This is ALWAYS safe because:
        - WHERE A followed by WHERE B ≡ WHERE (A AND B)
        - Pure boolean logic, no semantic edge cases

        CONSERVATIVE RULES:
        1. Both must have filter expressions
        2. Inner selection must have an input to connect to

        Returns:
            True if flattening is safe and beneficial.
        """
        # Both must have filters to merge
        if inner_sel.filter_expression is None:
            return False
        if outer_sel.filter_expression is None:
            return False

        # Inner must have an input to connect to
        if inner_sel.in_operator is None:
            return False

        return True

    def _merge_selection_into_selection(
        self,
        inner_sel: SelectionOperator,
        outer_sel: SelectionOperator,
    ) -> None:
        """Merge two consecutive SelectionOperators by ANDing their filters.

        BEFORE:
            OuterSelection(filter=B)._in_operators = [InnerSelection]
            InnerSelection(filter=A)._in_operators = [SomeOp]

        AFTER:
            OuterSelection(filter=A AND B)._in_operators = [SomeOp]
            InnerSelection is orphaned
        """
        inner_input = inner_sel.in_operator
        if inner_input is None:
            return

        # Combine filters: inner AND outer
        # (inner filter is evaluated first, then outer - order matters for short-circuit)
        and_operator = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
        combined_filter = QueryExpressionBinary(
            left_expression=inner_sel.filter_expression,
            operator=and_operator,
            right_expression=outer_sel.filter_expression,
        )
        outer_sel.filter_expression = combined_filter

        # Rewire the graph:
        # 1. Remove InnerSelection from its input's out_operators
        if inner_sel in inner_input._out_operators:
            inner_input._out_operators.remove(inner_sel)

        # 2. Add OuterSelection to input's out_operators
        if outer_sel not in inner_input._out_operators:
            inner_input._out_operators.append(outer_sel)

        # 3. Update OuterSelection's in_operators to point to InnerSelection's input
        outer_sel._in_operators = [inner_input]

        # 4. Clear InnerSelection's references (orphan it)
        inner_sel._in_operators = []
        inner_sel._out_operators = []

        # Update stats
        self.stats.selection_into_selection += 1


# =============================================================================
# Selection Pushdown Optimizer
# =============================================================================


@dataclass
class PushdownStats:
    """Statistics about selection pushdown operations performed."""

    predicates_pushed: int = 0
    predicates_remaining: int = 0
    selections_removed: int = 0

    def __str__(self) -> str:
        return (
            f"PushdownStats("
            f"pushed={self.predicates_pushed}, "
            f"remaining={self.predicates_remaining}, "
            f"selections_removed={self.selections_removed})"
        )


class SelectionPushdownOptimizer:
    """Pushes predicates from Selection operators into DataSource operators.

    This optimizer analyzes Selection operators and pushes predicates that
    reference only a single entity (node or relationship) down to the
    corresponding DataSourceOperator. This is especially important for
    undirected relationships where filters should be applied before joins.

    ==========================================================================
    SAFETY CHECKLIST - WHEN PUSHDOWN IS BLOCKED
    ==========================================================================

    This optimizer implements CONSERVATIVE pushdown. We block pushdown in
    several cases to preserve query semantics. Here's the complete checklist:

    ┌──────────────────────────────────────────────────────────────────────────┐
    │  CHECK                │ WHY BLOCKED                   │ IMPLEMENTED?    │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  refs.size() != 1     │ Multi-var predicates need     │ ✅ YES          │
    │                       │ both sides of join            │ _group_preds    │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  OR predicate         │ σ_{p∨q}(A⋈B) ≢ σ_p(A)⋈σ_q(B) │ ✅ YES          │
    │                       │ Splitting OR is semantically  │ _split_and only │
    │                       │ wrong                         │                 │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  NOT INNER JOIN       │ LEFT JOIN: pushing to right   │ ✅ YES          │
    │                       │ side changes OPTIONAL MATCH   │ _has_non_inner  │
    │                       │ semantics (NULL rows lost)    │ _join_in_path   │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  Recursive path       │ RecursiveTraversalOperator    │ ✅ YES          │
    │                       │ renderer doesn't support      │ _is_in_recursive│
    │                       │ filter_expression             │ _path           │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  Volatile functions   │ rand(), datetime() change on  │ ✅ YES          │
    │  (rand, datetime,     │ each call - pushing changes   │ _contains_      │
    │   now, etc.)          │ how often they're evaluated   │ volatile_func   │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  Aggregation funcs    │ COUNT, SUM, etc. cannot be    │ ✅ YES          │
    │                       │ pushed before grouping        │ _contains_      │
    │                       │                               │ aggregation     │
    ├──────────────────────────────────────────────────────────────────────────┤
    │  Correlated subquery  │ EXISTS/NOT EXISTS with outer  │ ❌ N/A          │
    │                       │ reference                     │ Not in parser   │
    └──────────────────────────────────────────────────────────────────────────┘

    TODO: Future safety checks to consider:
    - Window functions (if added to parser)
    - Non-deterministic UDFs (if UDF support is added)

    ==========================================================================
    CONJUNCTION SPLITTING FOR UNDIRECTED EDGES
    ==========================================================================

    For undirected edge patterns like:

        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 25

    The WHERE clause references TWO variables (p and f). Without conjunction
    splitting, the entire predicate stays in the SelectionOperator after joins.

    With conjunction splitting:
        1. Split "p.name = 'Alice' AND f.age > 25" into:
           - {p} → p.name = 'Alice'
           - {f} → f.age > 25
        2. Push p.name = 'Alice' to DataSource(p)
        3. Push f.age > 25 to DataSource(f)
        4. Remove the SelectionOperator entirely

    ==========================================================================
    WHAT WE PUSH (SAFE)
    ==========================================================================

    ✅ Single-variable predicates:
        WHERE p.name = 'Alice'              → Push to DataSource(p)
        WHERE p.age > 30 AND p.active       → Push combined to DataSource(p)

    ✅ Multi-variable AND conjunctions (after splitting):
        WHERE p.name = 'Alice' AND f.age > 25
            → Push p.name = 'Alice' to DataSource(p)
            → Push f.age > 25 to DataSource(f)

    ==========================================================================
    WHAT WE DON'T PUSH (KEEP IN SELECTION)
    ==========================================================================

    ❌ Cross-variable predicates:
        WHERE p.name = f.name               → Cannot push (references both p and f)

    ❌ OR predicates (even if single-variable inside):
        WHERE p.name = 'Alice' OR f.age > 25  → Cannot split OR safely

    ❌ Predicates for recursive path sources:
        MATCH (a)-[:KNOWS*1..3]->(b)
        WHERE a.name = 'Alice'              → Cannot push (recursive rendering)

    ❌ Predicates through LEFT JOINs (OPTIONAL MATCH):
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE f.age > 30                    → Cannot push to f (through LEFT JOIN)

    ❌ Volatile functions:
        WHERE rand() > 0.5                  → Cannot push (changes eval count)
        WHERE datetime() < p.created_at     → Cannot push (non-deterministic)

    ==========================================================================
    ALGEBRAIC FOUNDATION
    ==========================================================================

    The optimization is based on the relational algebra equivalence:

        σ_{p(A)}(A ⋈ B) ≡ σ_{p(A)}(A) ⋈ B

    Where:
        - σ_{p(A)} is a selection predicate referencing only attributes of A
        - A ⋈ B is a join between relations A and B

    For AND conjunctions:
        σ_{p(A) ∧ q(B)}(A ⋈ B) ≡ σ_{p(A)}(A) ⋈ σ_{q(B)}(B)

    This is NOT valid for OR:
        σ_{p(A) ∨ q(B)}(A ⋈ B) ≢ σ_{p(A)}(A) ⋈ σ_{q(B)}(B)  ← WRONG!

    ==========================================================================
    EXAMPLE
    ==========================================================================

        BEFORE (without pushdown):
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice' AND f.age > 25
            RETURN f.name

            Plan: DataSource(p) → Join(KNOWS) → Join(f) → Selection(p.name='Alice' AND f.age>25)

            SQL: SELECT ... FROM Person p JOIN Knows ... JOIN Person f WHERE p.name='Alice' AND f.age>25

        AFTER (with pushdown):
            Plan: DataSource(p, filter=name='Alice') → Join(KNOWS) → DataSource(f, filter=age>25)

            SQL: SELECT ... FROM (SELECT * FROM Person WHERE name='Alice') p
                 JOIN Knows ... JOIN (SELECT * FROM Person WHERE age>25) f

    This significantly reduces the number of rows processed in joins.
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the optimizer.

        Args:
            enabled: If False, optimize() becomes a no-op.
        """
        self.enabled = enabled
        self.stats = PushdownStats()

    def optimize(self, plan: LogicalPlan) -> None:
        """Apply selection pushdown optimization to the logical plan.

        Modifies the plan IN-PLACE.

        Args:
            plan: The logical plan to optimize.
        """
        if not self.enabled:
            return

        self.stats = PushdownStats()

        # Collect all operators
        all_operators: list[LogicalOperator] = []
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                if op not in all_operators:
                    all_operators.append(op)

        # Find all Selection operators
        selections = [op for op in all_operators if isinstance(op, SelectionOperator)]

        # Process each Selection
        for selection in selections:
            self._try_push_selection(selection, plan)

    def _try_push_selection(
        self, selection: SelectionOperator, plan: LogicalPlan
    ) -> None:
        """Try to push a Selection's predicates into DataSource operators.

        This method implements conjunction splitting for undirected edge patterns.

        Algorithm:
            1. Split the filter expression into individual AND conjuncts
            2. Group conjuncts by the variable(s) they reference
            3. For each single-variable group:
               - Find the target DataSource
               - Push the combined predicates to that DataSource
            4. Reconstruct the Selection with remaining predicates (if any)
               or remove it entirely

        Args:
            selection: The Selection operator to analyze.
            plan: The full logical plan (for finding DataSources).
        """
        if not selection.filter_expression:
            return

        # =======================================================================
        # Step 1: Split AND conjunctions
        # =======================================================================
        # "p.name = 'Alice' AND f.age > 25 AND p.active = true"
        # becomes: [p.name = 'Alice', f.age > 25, p.active = true]
        conjuncts = self._split_and_conjunctions(selection.filter_expression)

        # =======================================================================
        # Step 2: Group predicates by variable
        # =======================================================================
        # Groups:
        #   "p" → [p.name = 'Alice', p.active = true]
        #   "f" → [f.age > 25]
        #   None → [predicates referencing multiple variables]
        groups = self._group_predicates_by_variable(conjuncts)

        # =======================================================================
        # Step 3: Push each single-variable group to its DataSource
        # =======================================================================
        pushed_any = False
        remaining_predicates: list[QueryExpression] = []

        for var_name, predicates in groups.items():
            if var_name is None:
                # Multi-variable predicates cannot be pushed
                # TODO: Future optimization - analyze if we can push part of complex
                # predicates (e.g., COALESCE with fallback values)
                remaining_predicates.extend(predicates)
                self.stats.predicates_remaining += len(predicates)
                continue

            # Find the DataSourceOperator for this variable
            # Pass selection to check for LEFT/OUTER joins in path
            target_ds = self._find_datasource_for_variable(var_name, plan, selection)
            if not target_ds:
                # Cannot find target or target is in recursive path
                remaining_predicates.extend(predicates)
                self.stats.predicates_remaining += len(predicates)
                continue

            # Combine all predicates for this variable with AND
            combined = self._combine_predicates_with_and(predicates)
            if combined is None:
                continue

            # Push to the DataSource
            self._push_predicate_to_datasource(combined, target_ds)
            self.stats.predicates_pushed += len(predicates)
            pushed_any = True

        # =======================================================================
        # Step 4: Handle remaining predicates or remove Selection
        # =======================================================================
        if not remaining_predicates:
            # All predicates were pushed - remove the Selection entirely
            if pushed_any:
                self._remove_selection(selection)
                self.stats.selections_removed += 1
        else:
            # Some predicates couldn't be pushed - update the Selection
            if pushed_any:
                # Reconstruct filter with only the remaining predicates
                new_filter = self._combine_predicates_with_and(remaining_predicates)
                selection.filter_expression = new_filter
                # NOTE: We keep the Selection operator in place with the reduced filter

    # =========================================================================
    # Conjunction Splitting Helpers
    # =========================================================================

    def _split_and_conjunctions(
        self, expr: QueryExpression
    ) -> list[QueryExpression]:
        """Split an expression into individual AND conjuncts.

        Recursively splits AND expressions into a flat list of predicates.
        Does NOT split OR expressions (they must be kept together).

        Examples:
            "A AND B AND C" → [A, B, C]
            "A AND (B OR C)" → [A, (B OR C)]
            "A OR B" → [A OR B]  (not split)
            "(A AND B) AND (C AND D)" → [A, B, C, D]

        Args:
            expr: The expression to split.

        Returns:
            List of individual predicates (conjuncts).
        """
        if isinstance(expr, QueryExpressionBinary):
            # Check if this is an AND expression
            # BinaryOperatorInfo has 'name' attribute (not 'binary_operator')
            if (
                expr.operator is not None
                and expr.operator.name == BinaryOperator.AND
                and expr.left_expression is not None
                and expr.right_expression is not None
            ):
                # Recursively split both sides
                left_conjuncts = self._split_and_conjunctions(expr.left_expression)
                right_conjuncts = self._split_and_conjunctions(expr.right_expression)
                return left_conjuncts + right_conjuncts

        # Not an AND expression - return as single predicate
        return [expr]

    def _group_predicates_by_variable(
        self, predicates: list[QueryExpression]
    ) -> dict[str | None, list[QueryExpression]]:
        """Group predicates by the variable they reference.

        Predicates referencing a single variable are grouped under that variable.
        Predicates referencing multiple variables are grouped under None.
        Predicates containing volatile or aggregation functions are also grouped
        under None (cannot be pushed).

        Examples:
            [p.name='Alice', f.age>25, p.active=true, p.name=f.name, rand()>0.5]
            →
            {
                'p': [p.name='Alice', p.active=true],
                'f': [f.age>25],
                None: [p.name=f.name, rand()>0.5]  # multi-variable or volatile
            }

        Args:
            predicates: List of predicate expressions.

        Returns:
            Dictionary mapping variable name (or None) to predicates.
        """
        groups: dict[str | None, list[QueryExpression]] = {}

        for pred in predicates:
            # =================================================================
            # Safety Check 1: Volatile functions (rand, datetime, etc.)
            # =================================================================
            # Predicates with volatile functions CANNOT be pushed because
            # pushing changes when/how often they're evaluated.
            # Example: WHERE rand() > 0.5 - evaluated once per final row vs
            #          once per source row if pushed (different result!)
            if self._contains_volatile_function(pred):
                groups.setdefault(None, []).append(pred)
                continue

            # =================================================================
            # Safety Check 2: Aggregation functions (COUNT, SUM, etc.)
            # =================================================================
            # Predicates with aggregation CANNOT be pushed because they must
            # be evaluated after grouping. (Rare in WHERE, but check anyway)
            if self._contains_aggregation_function(pred):
                groups.setdefault(None, []).append(pred)
                continue

            # =================================================================
            # Safety Check 3: Variable count
            # =================================================================
            # Collect all property references in this predicate
            properties = self._collect_property_references(pred)

            # Get unique variable names
            var_names = {p.variable_name for p in properties}

            if len(var_names) == 1:
                # Single-variable predicate - can be pushed
                var_name = next(iter(var_names))
                groups.setdefault(var_name, []).append(pred)
            elif len(var_names) == 0:
                # No variables (constant expression like "1 = 1")
                # Keep in Selection for safety
                # TODO: Could potentially push to any DataSource or eliminate
                # FIX: If it's "1 = 1", could simplify. If "1 = 0", short-circuit.
                groups.setdefault(None, []).append(pred)
            else:
                # Multi-variable predicate - cannot push
                groups.setdefault(None, []).append(pred)

        return groups

    def _combine_predicates_with_and(
        self, predicates: list[QueryExpression]
    ) -> QueryExpression | None:
        """Combine a list of predicates using AND.

        Args:
            predicates: List of predicates to combine.

        Returns:
            Combined expression, or None if list is empty.
        """
        if not predicates:
            return None

        if len(predicates) == 1:
            return predicates[0]

        # Build AND chain: ((A AND B) AND C) AND D
        and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)

        result = predicates[0]
        for pred in predicates[1:]:
            result = QueryExpressionBinary(
                left_expression=result,
                operator=and_op,
                right_expression=pred,
            )

        return result

    def _push_predicate_to_datasource(
        self, predicate: QueryExpression, target_ds: DataSourceOperator
    ) -> None:
        """Push a predicate to a DataSource's filter_expression.

        Combines with existing filter if present.

        Args:
            predicate: The predicate to push.
            target_ds: The target DataSource operator.
        """
        if target_ds.filter_expression is None:
            target_ds.filter_expression = predicate
        else:
            # Combine with existing filter using AND
            and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
            target_ds.filter_expression = QueryExpressionBinary(
                left_expression=target_ds.filter_expression,
                operator=and_op,
                right_expression=predicate,
            )

    # =========================================================================
    # Original Helper Methods
    # =========================================================================

    def _collect_property_references(
        self,
        expr: QueryExpression,
    ) -> list:
        """Collect all property references from an expression tree.

        Args:
            expr: The expression to analyze.

        Returns:
            List of QueryExpressionProperty objects found in the expression.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionProperty,
            QueryExpressionBinary,
            QueryExpressionFunction,
            QueryExpressionAggregationFunction,
            QueryExpressionCaseExpression,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        properties: list = []

        if isinstance(expr, QueryExpressionProperty):
            properties.append(expr)
        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression:
                properties.extend(self._collect_property_references(expr.left_expression))
            if expr.right_expression:
                properties.extend(self._collect_property_references(expr.right_expression))
        elif isinstance(expr, QueryExpressionFunction):
            for arg in expr.parameters or []:
                properties.extend(self._collect_property_references(arg))
        elif isinstance(expr, QueryExpressionAggregationFunction):
            if expr.inner_expression:
                properties.extend(self._collect_property_references(expr.inner_expression))
        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression:
                properties.extend(self._collect_property_references(expr.test_expression))
            for when_expr, then_expr in expr.alternatives or []:
                properties.extend(self._collect_property_references(when_expr))
                properties.extend(self._collect_property_references(then_expr))
            if expr.else_expression:
                properties.extend(self._collect_property_references(expr.else_expression))
        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                properties.extend(self._collect_property_references(item))
        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression:
                properties.extend(self._collect_property_references(expr.list_expression))
            if expr.predicate_expression:
                properties.extend(self._collect_property_references(expr.predicate_expression))
        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression:
                properties.extend(self._collect_property_references(expr.inner_expression))
        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression):
                    properties.extend(self._collect_property_references(child))

        return properties

    # =========================================================================
    # Volatile Function and Aggregation Detection
    # =========================================================================

    # Volatile functions: result changes on each call even with same arguments
    # These CANNOT be pushed because:
    #   - Before pushdown: evaluated once per row AFTER join
    #   - After pushdown: evaluated once per row BEFORE join (different row count!)
    #
    # Example of semantic change:
    #   WHERE rand() > 0.5 AND p.name = 'Alice'
    #   Correct (not pushed): filter 50% of joined rows randomly
    #   Wrong (pushed): filter 50% of Person rows, THEN join (more rows filtered!)
    VOLATILE_FUNCTIONS: set["Function"] = {
        Function.RAND,           # Random number generation
        Function.DATE,           # When called without args: current date
        Function.DATETIME,       # When called without args: current datetime
        Function.LOCALDATETIME,  # When called without args: current local datetime
        Function.TIME,           # When called without args: current time
        Function.LOCALTIME,      # When called without args: current local time
    }

    def _contains_volatile_function(self, expr: QueryExpression) -> bool:
        """Check if expression contains any volatile (non-deterministic) function.

        Volatile functions like rand(), datetime() etc. produce different results
        on each call. Pushing predicates with volatile functions changes when/how
        often they're evaluated, altering query semantics.

        Example:
            WHERE rand() > 0.5
            - Not pushed: evaluated once per FINAL row (after joins)
            - If pushed: evaluated once per SOURCE row (before joins, different count!)

        Note: Date/time functions are volatile when called without arguments
        (they return current time). When called with arguments like
        datetime({year:2020, month:1, day:1}), they're deterministic.
        However, we conservatively block all calls since analyzing arguments
        is complex and the performance gain is minimal.

        Args:
            expr: The expression to analyze.

        Returns:
            True if the expression contains any volatile function.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionBinary,
            QueryExpressionFunction,
            QueryExpressionCaseExpression,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        if isinstance(expr, QueryExpressionFunction):
            if expr.function in self.VOLATILE_FUNCTIONS:
                return True
            # Check function arguments recursively
            for arg in expr.parameters or []:
                if self._contains_volatile_function(arg):
                    return True

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression and self._contains_volatile_function(expr.left_expression):
                return True
            if expr.right_expression and self._contains_volatile_function(expr.right_expression):
                return True

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression and self._contains_volatile_function(expr.test_expression):
                return True
            for when_expr, then_expr in expr.alternatives or []:
                if self._contains_volatile_function(when_expr):
                    return True
                if self._contains_volatile_function(then_expr):
                    return True
            if expr.else_expression and self._contains_volatile_function(expr.else_expression):
                return True

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                if self._contains_volatile_function(item):
                    return True

        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression and self._contains_volatile_function(expr.list_expression):
                return True
            if expr.filter_expression and self._contains_volatile_function(expr.filter_expression):
                return True

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression and self._contains_volatile_function(expr.inner_expression):
                return True

        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression) and self._contains_volatile_function(child):
                    return True

        return False

    def _contains_aggregation_function(self, expr: QueryExpression) -> bool:
        """Check if expression contains any aggregation function.

        Aggregation functions (COUNT, SUM, AVG, etc.) cannot be pushed because
        they must be evaluated AFTER grouping, not before.

        Note: In practice, aggregation functions in WHERE clauses are rare
        and typically a semantic error. But we check defensively.

        Args:
            expr: The expression to analyze.

        Returns:
            True if the expression contains any aggregation function.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionBinary,
            QueryExpressionFunction,
            QueryExpressionAggregationFunction,
            QueryExpressionCaseExpression,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        if isinstance(expr, QueryExpressionAggregationFunction):
            return True

        elif isinstance(expr, QueryExpressionFunction):
            for arg in expr.parameters or []:
                if self._contains_aggregation_function(arg):
                    return True

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression and self._contains_aggregation_function(expr.left_expression):
                return True
            if expr.right_expression and self._contains_aggregation_function(expr.right_expression):
                return True

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression and self._contains_aggregation_function(expr.test_expression):
                return True
            for when_expr, then_expr in expr.alternatives or []:
                if self._contains_aggregation_function(when_expr):
                    return True
                if self._contains_aggregation_function(then_expr):
                    return True
            if expr.else_expression and self._contains_aggregation_function(expr.else_expression):
                return True

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                if self._contains_aggregation_function(item):
                    return True

        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression and self._contains_aggregation_function(expr.list_expression):
                return True
            if expr.filter_expression and self._contains_aggregation_function(expr.filter_expression):
                return True

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression and self._contains_aggregation_function(expr.inner_expression):
                return True

        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression) and self._contains_aggregation_function(child):
                    return True

        return False

    def _find_datasource_for_variable(
        self, variable: str, plan: LogicalPlan, selection: SelectionOperator | None = None
    ) -> DataSourceOperator | None:
        """Find the DataSourceOperator that provides a given variable.

        Only returns DataSources that can have filters pushed to them.
        Excludes DataSources involved in:
        - Recursive path patterns (renderer doesn't support filter_expression)
        - LEFT/OUTER join paths (pushdown would change semantics)

        Args:
            variable: The variable name (e.g., 'p' from 'p:Person').
            plan: The logical plan to search.
            selection: The Selection operator we're pushing from (for join path check).

        Returns:
            The DataSourceOperator for the variable, or None if not found
            or if the DataSource can't have filters pushed to it.
        """
        for start_op in plan.starting_operators:
            if isinstance(start_op, DataSourceOperator):
                if start_op.entity and start_op.entity.alias == variable:
                    # Check if this DataSource is involved in a recursive path
                    # If so, don't push (the renderer doesn't support it)
                    if self._is_in_recursive_path(start_op):
                        return None

                    # Check if path to Selection contains non-INNER joins
                    # Pushing through LEFT/OUTER joins changes semantics!
                    # Example: OPTIONAL MATCH creates LEFT JOIN - pushing filter
                    # to the optional side would incorrectly filter before the join.
                    if selection and self._has_non_inner_join_in_path(start_op, selection):
                        return None

                    return start_op
        return None

    def _has_non_inner_join_in_path(
        self, ds: DataSourceOperator, selection: SelectionOperator
    ) -> bool:
        """Check if DataSource is on the RIGHT (optional) side of a LEFT JOIN.

        Pushing predicates through LEFT JOINs is only unsafe when the variable
        is on the RIGHT (optional) side of the join. The LEFT side is preserved.

        Example:
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
            WHERE f.age > 30

        Here f is on the RIGHT side of a LEFT JOIN:
            JOIN(type=LEFT, left=p, right=pattern_with_f)

        If we push f.age > 30 to DataSource(f), we filter BEFORE the LEFT JOIN,
        which gives different results than filtering AFTER.

        But p.age > 30 CAN be pushed because p is on the LEFT side (preserved).

        Args:
            ds: The DataSourceOperator we want to push to.
            selection: The Selection operator we're pushing from.

        Returns:
            True if ds is on the optional (RIGHT) side of a LEFT JOIN (unsafe).
        """
        # BFS/DFS from ds to selection, tracking if we're on the optional side
        visited: set[int] = set()
        return self._is_on_optional_side_of_left_join(ds, selection, visited)

    def _is_on_optional_side_of_left_join(
        self,
        current: LogicalOperator,
        target: SelectionOperator,
        visited: set[int],
    ) -> bool:
        """Check if current operator is on the optional side of a LEFT JOIN.

        For LEFT JOINs:
        - Left input (in_operator_left) is the PRESERVED side → safe to push
        - Right input (in_operator_right) is the OPTIONAL side → NOT safe to push

        Args:
            current: Current operator in traversal.
            target: Target Selection operator.
            visited: Set of visited operator IDs.

        Returns:
            True if current is on the optional (right) side of a LEFT JOIN.
        """
        op_id = id(current)
        if op_id in visited:
            return False
        visited.add(op_id)

        # If we reached the target, we didn't find problematic LEFT joins
        if current is target:
            return False

        # Check all downstream operators
        for out_op in current._out_operators:
            if isinstance(out_op, JoinOperator):
                if out_op.join_type == JoinType.LEFT:
                    # Check if we're coming from the RIGHT (optional) side
                    # JoinOperator has in_operator_left and in_operator_right
                    is_right_input = self._is_input_of_join(current, out_op, is_right=True)
                    if is_right_input:
                        # We're on the optional side of a LEFT JOIN
                        # Check if the target is reachable (to confirm this join is in path)
                        if self._can_reach_operator(out_op, target, set()):
                            return True

                # For CROSS joins, any side is potentially problematic
                if out_op.join_type == JoinType.CROSS:
                    if self._can_reach_operator(out_op, target, set()):
                        # CROSS joins are rare in Cypher, but be conservative
                        return True

            # Continue searching downstream
            if self._is_on_optional_side_of_left_join(out_op, target, visited):
                return True

        return False

    def _is_input_of_join(
        self, op: LogicalOperator, join_op: JoinOperator, is_right: bool
    ) -> bool:
        """Check if operator is a specific input (left or right) of a join.

        Args:
            op: The operator to check.
            join_op: The JoinOperator.
            is_right: If True, check if op is right input. If False, check left.

        Returns:
            True if op is the specified input of the join.
        """
        # JoinOperator inherits from BinaryLogicalOperator
        # which has in_operator_left and in_operator_right properties
        if is_right:
            target_input = join_op.in_operator_right
        else:
            target_input = join_op.in_operator_left

        if target_input is None:
            return False

        # Check if op is the target input or an ancestor of it
        return op is target_input or self._is_ancestor_of(op, target_input)

    def _is_ancestor_of(self, ancestor: LogicalOperator, descendant: LogicalOperator) -> bool:
        """Check if ancestor is an upstream operator of descendant.

        Args:
            ancestor: Potential ancestor operator.
            descendant: Potential descendant operator.

        Returns:
            True if ancestor is upstream of descendant.
        """
        # Check all inputs of descendant
        for in_op in descendant._in_operators:
            if in_op is ancestor:
                return True
            if self._is_ancestor_of(ancestor, in_op):
                return True
        return False

    def _can_reach_operator(
        self,
        start: LogicalOperator,
        target: LogicalOperator,
        visited: set[int],
    ) -> bool:
        """Check if target is reachable from start.

        Args:
            start: Starting operator.
            target: Target operator to find.
            visited: Set of visited operator IDs.

        Returns:
            True if target is reachable from start.
        """
        op_id = id(start)
        if op_id in visited:
            return False
        visited.add(op_id)

        if start is target:
            return True

        for out_op in start._out_operators:
            if self._can_reach_operator(out_op, target, visited):
                return True

        return False

    def _is_in_recursive_path(self, ds: DataSourceOperator) -> bool:
        """Check if a DataSource is involved in a recursive path pattern.

        DataSources that feed into or are joined with RecursiveTraversalOperators
        are rendered differently and don't support filter_expression pushdown.

        Args:
            ds: The DataSourceOperator to check.

        Returns:
            True if the DataSource is part of a recursive path pattern.
        """
        # Check all downstream operators
        for out_op in ds._out_operators:
            if isinstance(out_op, RecursiveTraversalOperator):
                return True
            if isinstance(out_op, JoinOperator):
                # Check if the join's other input is a RecursiveTraversalOperator
                for in_op in out_op._in_operators:
                    if isinstance(in_op, RecursiveTraversalOperator):
                        return True
                    # Also check for joins that have recursive as ancestor
                    if self._has_recursive_ancestor(in_op):
                        return True
        return False

    def _has_recursive_ancestor(self, op: LogicalOperator) -> bool:
        """Check if an operator has a RecursiveTraversalOperator as ancestor.

        Args:
            op: The operator to check.

        Returns:
            True if there's a RecursiveTraversalOperator upstream.
        """
        if isinstance(op, RecursiveTraversalOperator):
            return True
        for in_op in op._in_operators:
            if self._has_recursive_ancestor(in_op):
                return True
        return False

    def _remove_selection(self, selection: SelectionOperator) -> None:
        """Remove a Selection operator from the plan by bypassing it.

        Connects the Selection's input directly to its outputs.

        Args:
            selection: The Selection operator to remove.
        """
        in_op = selection.in_operator
        if not in_op:
            return

        # Connect in_op directly to all of selection's out_operators
        for out_op in selection._out_operators:
            # Update out_op's in_operators to point to in_op
            if selection in out_op._in_operators:
                idx = out_op._in_operators.index(selection)
                out_op._in_operators[idx] = in_op

            # Add out_op to in_op's out_operators
            if out_op not in in_op._out_operators:
                in_op._out_operators.append(out_op)

        # Remove selection from in_op's out_operators
        if selection in in_op._out_operators:
            in_op._out_operators.remove(selection)

        # Orphan the selection
        selection._in_operators = []
        selection._out_operators = []


def optimize_plan(
    plan: LogicalPlan,
    enabled: bool = True,
    pushdown_enabled: bool = True,
) -> FlatteningStats:
    """Convenience function to optimize a logical plan.

    Runs two optimization passes:
    1. SelectionPushdownOptimizer: Pushes predicates into DataSource operators
    2. SubqueryFlatteningOptimizer: Merges Selection → Projection patterns

    Args:
        plan: The logical plan to optimize.
        enabled: Whether to run subquery flattening optimization.
        pushdown_enabled: Whether to run selection pushdown optimization.

    Returns:
        Statistics about the flattening optimization performed.
    """
    # Run selection pushdown first (before flattening)
    # This pushes WHERE predicates into DataSource operators
    if pushdown_enabled:
        pushdown_optimizer = SelectionPushdownOptimizer(enabled=True)
        pushdown_optimizer.optimize(plan)

    # Then run subquery flattening
    flattening_optimizer = SubqueryFlatteningOptimizer(enabled=enabled)
    flattening_optimizer.optimize(plan)

    return flattening_optimizer.stats
