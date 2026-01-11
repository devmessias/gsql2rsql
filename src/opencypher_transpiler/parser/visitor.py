"""ANTLR4 visitor for converting parse tree to AST."""

from __future__ import annotations

from typing import Any

from opencypher_transpiler.common.exceptions import TranspilerSyntaxErrorException
from opencypher_transpiler.common.logging import ILoggable
from opencypher_transpiler.parser.ast import (
    Entity,
    InfixOperator,
    InfixQueryNode,
    LimitClause,
    MatchClause,
    NodeEntity,
    PartialQueryNode,
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionCaseExpression,
    QueryExpressionFunction,
    QueryExpressionList,
    QueryExpressionProperty,
    QueryExpressionValue,
    QueryExpressionWithAlias,
    QueryNode,
    RelationshipDirection,
    RelationshipEntity,
    SingleQueryNode,
    SortItem,
    SortOrder,
)
from opencypher_transpiler.parser.operators import (
    Function,
    try_get_function,
    try_get_operator,
    try_parse_aggregation_function,
)


class CypherVisitor:
    """
    Visitor that converts ANTLR parse tree to openCypher AST.

    This class implements the visitor pattern to walk the ANTLR-generated
    parse tree and construct our custom AST representation.
    """

    def __init__(self, logger: ILoggable | None = None) -> None:
        self._logger = logger

    def visit(self, tree: Any) -> Any:
        """
        Visit a parse tree node and dispatch to the appropriate handler.

        This is the main entry point for visiting parse tree nodes.
        """
        if tree is None:
            return None

        # Get the visitor method based on the node type
        class_name = tree.__class__.__name__

        # Handle context names like 'OC_CypherContext' -> 'visit_oC_Cypher'
        if class_name.endswith("Context"):
            # Remove 'Context' suffix and convert 'OC_' to 'oC_'
            rule_name = class_name[:-7]
            if rule_name.startswith("OC_"):
                rule_name = "oC_" + rule_name[3:]
            method_name = f"visit_{rule_name}"
        else:
            method_name = f"visit_{class_name}"

        visitor_method = getattr(self, method_name, None)

        if visitor_method:
            return visitor_method(tree)

        # Default: visit children and return last result
        return self.visit_children(tree)

    def visit_children(self, tree: Any) -> Any:
        """Visit all children of a node and return the last result."""
        result: Any = None
        for i in range(tree.getChildCount()):
            child = tree.getChild(i)
            # Skip whitespace/terminal nodes
            if not hasattr(child, "getRuleIndex"):
                continue
            child_result = self.visit(child)
            if child_result is not None:
                result = child_result
        return result

    # =========================================================================
    # Top-level query visitors
    # =========================================================================

    def visit_oC_Cypher(self, ctx: Any) -> QueryNode:
        """Visit the root Cypher context."""
        return self.visit(ctx.oC_Statement())

    def visit_oC_Statement(self, ctx: Any) -> QueryNode:
        """Visit a statement context."""
        return self.visit(ctx.oC_Query())

    def visit_oC_Query(self, ctx: Any) -> QueryNode:
        """Visit a query context."""
        if ctx.oC_RegularQuery():
            return self.visit(ctx.oC_RegularQuery())
        return SingleQueryNode()

    def visit_oC_RegularQuery(self, ctx: Any) -> QueryNode:
        """Visit a regular query context."""
        result: QueryNode = self.visit(ctx.oC_SingleQuery())

        # Handle UNION clauses
        for union_ctx in ctx.oC_Union() or []:
            right = self.visit(union_ctx.oC_SingleQuery())
            has_all = any(
                str(child.getText()).upper() == "ALL"
                for i in range(union_ctx.getChildCount())
                if hasattr((child := union_ctx.getChild(i)), "getText")
            )
            operator = InfixOperator.UNION_ALL if has_all else InfixOperator.UNION
            result = InfixQueryNode(
                operator=operator,
                left_query=result,
                right_query=right,
            )

        return result

    def visit_oC_SingleQuery(self, ctx: Any) -> SingleQueryNode:
        """Visit a single query context."""
        parts: list[PartialQueryNode] = []

        # Visit single part query if present
        if ctx.oC_SinglePartQuery():
            part = self.visit(ctx.oC_SinglePartQuery())
            if isinstance(part, PartialQueryNode):
                parts.append(part)

        # Visit multi-part query if present
        if ctx.oC_MultiPartQuery():
            multi_parts = self.visit(ctx.oC_MultiPartQuery())
            if isinstance(multi_parts, list):
                parts.extend(multi_parts)
            elif isinstance(multi_parts, PartialQueryNode):
                parts.append(multi_parts)

        return SingleQueryNode(parts=parts)

    def visit_oC_SinglePartQuery(self, ctx: Any) -> PartialQueryNode:
        """Visit a single part query context."""
        part = PartialQueryNode()

        # Visit reading clauses
        for reading_ctx in ctx.oC_ReadingClause() or []:
            reading = self.visit(reading_ctx)
            if isinstance(reading, MatchClause):
                part.match_clauses.append(reading)

        # Visit return clause
        if ctx.oC_Return():
            return_result = self.visit(ctx.oC_Return())
            if return_result:
                self._apply_return_result(part, return_result)

        return part

    def _apply_return_result(self, part: PartialQueryNode, result: dict[str, Any]) -> None:
        """Apply return clause results to a partial query node."""
        if "body" in result:
            part.return_body = result["body"]
        if "distinct" in result:
            part.is_distinct = result["distinct"]
        if "order" in result:
            part.order_by = result["order"]
        if "limit" in result:
            part.limit_clause = result["limit"]

    # =========================================================================
    # Reading clause visitors
    # =========================================================================

    def visit_oC_ReadingClause(self, ctx: Any) -> MatchClause | None:
        """Visit a reading clause context."""
        if ctx.oC_Match():
            return self.visit(ctx.oC_Match())
        return None

    def visit_oC_Match(self, ctx: Any) -> MatchClause:
        """Visit a MATCH clause context."""
        is_optional = any(
            str(child.getText()).upper() == "OPTIONAL"
            for i in range(ctx.getChildCount())
            if hasattr((child := ctx.getChild(i)), "getText")
            and not hasattr(child, "getRuleIndex")
        )

        pattern = self.visit(ctx.oC_Pattern()) if ctx.oC_Pattern() else []
        where_expr = self.visit(ctx.oC_Where()) if ctx.oC_Where() else None

        return MatchClause(
            pattern_parts=pattern if isinstance(pattern, list) else [pattern],
            is_optional=is_optional,
            where_expression=where_expr,
        )

    def visit_oC_Pattern(self, ctx: Any) -> list[Entity]:
        """Visit a pattern context."""
        entities: list[Entity] = []
        for part_ctx in ctx.oC_PatternPart() or []:
            part = self.visit(part_ctx)
            if isinstance(part, list):
                entities.extend(part)
            elif isinstance(part, Entity):
                entities.append(part)
        return entities

    def visit_oC_PatternPart(self, ctx: Any) -> list[Entity]:
        """Visit a pattern part context."""
        return self.visit(ctx.oC_AnonymousPatternPart())

    def visit_oC_AnonymousPatternPart(self, ctx: Any) -> list[Entity]:
        """Visit an anonymous pattern part context."""
        return self.visit(ctx.oC_PatternElement())

    def visit_oC_PatternElement(self, ctx: Any) -> list[Entity]:
        """Visit a pattern element context."""
        entities: list[Entity] = []

        # Get the node pattern
        if ctx.oC_NodePattern():
            node = self.visit(ctx.oC_NodePattern())
            if isinstance(node, NodeEntity):
                entities.append(node)

        # Get the chain elements
        for chain_ctx in ctx.oC_PatternElementChain() or []:
            chain = self.visit(chain_ctx)
            if isinstance(chain, list):
                entities.extend(chain)

        # Handle nested pattern element
        if ctx.oC_PatternElement():
            nested = self.visit(ctx.oC_PatternElement())
            if isinstance(nested, list):
                entities.extend(nested)

        return entities

    def visit_oC_NodePattern(self, ctx: Any) -> NodeEntity:
        """Visit a node pattern context."""
        alias = ""
        entity_name = ""

        if ctx.oC_Variable():
            alias = ctx.oC_Variable().getText()

        if ctx.oC_NodeLabels():
            labels = ctx.oC_NodeLabels().getText()
            # Remove leading colon
            entity_name = labels.lstrip(":")
            # Take first label if multiple
            if ":" in entity_name:
                entity_name = entity_name.split(":")[0]

        return NodeEntity(alias=alias, entity_name=entity_name)

    def visit_oC_PatternElementChain(self, ctx: Any) -> list[Entity]:
        """Visit a pattern element chain context."""
        entities: list[Entity] = []

        if ctx.oC_RelationshipPattern():
            rel = self.visit(ctx.oC_RelationshipPattern())
            if isinstance(rel, RelationshipEntity):
                entities.append(rel)

        if ctx.oC_NodePattern():
            node = self.visit(ctx.oC_NodePattern())
            if isinstance(node, NodeEntity):
                # Update relationship's connected entities
                if entities and isinstance(entities[-1], RelationshipEntity):
                    entities[-1].right_entity_name = node.entity_name
                entities.append(node)

        return entities

    def visit_oC_RelationshipPattern(self, ctx: Any) -> RelationshipEntity:
        """Visit a relationship pattern context."""
        # Determine direction
        text = ctx.getText()
        if "->" in text:
            direction = RelationshipDirection.FORWARD
        elif "<-" in text:
            direction = RelationshipDirection.BACKWARD
        else:
            direction = RelationshipDirection.BOTH

        # Get details
        detail_ctx = ctx.oC_RelationshipDetail() if ctx.oC_RelationshipDetail() else None

        alias = ""
        entity_name = ""

        if detail_ctx:
            if detail_ctx.oC_Variable():
                alias = detail_ctx.oC_Variable().getText()
            if detail_ctx.oC_RelationshipTypes():
                types_text = detail_ctx.oC_RelationshipTypes().getText()
                entity_name = types_text.lstrip(":")

        return RelationshipEntity(
            alias=alias,
            entity_name=entity_name,
            direction=direction,
        )

    # =========================================================================
    # WHERE clause visitor
    # =========================================================================

    def visit_oC_Where(self, ctx: Any) -> QueryExpression | None:
        """Visit a WHERE clause context."""
        if ctx.oC_Expression():
            return self.visit(ctx.oC_Expression())
        return None

    # =========================================================================
    # RETURN clause visitors
    # =========================================================================

    def visit_oC_Return(self, ctx: Any) -> dict[str, Any]:
        """Visit a RETURN clause context."""
        result: dict[str, Any] = {}

        # New grammar uses oC_ProjectionBody directly in oC_Return
        if hasattr(ctx, "oC_ProjectionBody") and ctx.oC_ProjectionBody():
            body_result = self.visit(ctx.oC_ProjectionBody())
            if isinstance(body_result, dict):
                result.update(body_result)
        # Fallback for old grammar structure
        elif hasattr(ctx, "oC_ReturnBody") and ctx.oC_ReturnBody():
            body_result = self.visit(ctx.oC_ReturnBody())
            if isinstance(body_result, dict):
                result.update(body_result)

        return result

    def visit_oC_ProjectionBody(self, ctx: Any) -> dict[str, Any]:
        """Visit a projection body context (new grammar)."""
        result: dict[str, Any] = {}

        # Check for DISTINCT keyword
        text_children = [
            ctx.getChild(i).getText()
            for i in range(ctx.getChildCount())
            if hasattr(ctx.getChild(i), "getText")
            and not hasattr(ctx.getChild(i), "getRuleIndex")
        ]
        result["distinct"] = any(t.upper() == "DISTINCT" for t in text_children)

        if ctx.oC_ProjectionItems():
            items = self.visit(ctx.oC_ProjectionItems())
            if isinstance(items, list):
                result["body"] = items

        if ctx.oC_Order():
            order = self.visit(ctx.oC_Order())
            if isinstance(order, list):
                result["order"] = order

        if ctx.oC_Limit():
            limit = self.visit(ctx.oC_Limit())
            result["limit"] = LimitClause(limit_expression=limit)

        if ctx.oC_Skip():
            skip = self.visit(ctx.oC_Skip())
            if "limit" in result:
                result["limit"].skip_expression = skip
            else:
                result["limit"] = LimitClause(
                    limit_expression=QueryExpressionValue(
                        value=None, value_type=type(None)
                    ),
                    skip_expression=skip,
                )

        return result

    def visit_oC_ReturnBody(self, ctx: Any) -> dict[str, Any]:
        """Visit a return body context (legacy)."""
        result: dict[str, Any] = {}

        if ctx.oC_ReturnItems():
            items = self.visit(ctx.oC_ReturnItems())
            if isinstance(items, list):
                result["body"] = items

        if ctx.oC_Order():
            order = self.visit(ctx.oC_Order())
            if isinstance(order, list):
                result["order"] = order

        if ctx.oC_Limit():
            limit = self.visit(ctx.oC_Limit())
            result["limit"] = LimitClause(limit_expression=limit)

        if ctx.oC_Skip():
            skip = self.visit(ctx.oC_Skip())
            if "limit" in result:
                result["limit"].skip_expression = skip
            else:
                result["limit"] = LimitClause(
                    limit_expression=QueryExpressionValue(
                        value=None, value_type=type(None)
                    ),
                    skip_expression=skip,
                )

        return result

    def visit_oC_ProjectionItems(self, ctx: Any) -> list[QueryExpressionWithAlias]:
        """Visit projection items context."""
        items: list[QueryExpressionWithAlias] = []

        for item_ctx in ctx.oC_ProjectionItem() or []:
            item = self.visit(item_ctx)
            if isinstance(item, QueryExpressionWithAlias):
                items.append(item)

        return items

    def visit_oC_ProjectionItem(self, ctx: Any) -> QueryExpressionWithAlias | None:
        """Visit a projection item context."""
        expr = self.visit(ctx.oC_Expression())

        if not isinstance(expr, QueryExpression):
            return None

        alias = None
        if ctx.oC_Variable():
            alias = ctx.oC_Variable().getText()
        elif isinstance(expr, QueryExpressionProperty):
            alias = expr.property_name or expr.variable_name

        return QueryExpressionWithAlias(inner_expression=expr, alias=alias or "")

    def visit_oC_ReturnItems(self, ctx: Any) -> list[QueryExpressionWithAlias]:
        """Visit return items context (legacy)."""
        items: list[QueryExpressionWithAlias] = []

        for item_ctx in ctx.oC_ReturnItem() or []:
            item = self.visit(item_ctx)
            if isinstance(item, QueryExpressionWithAlias):
                items.append(item)

        return items

    def visit_oC_ReturnItem(self, ctx: Any) -> QueryExpressionWithAlias | None:
        """Visit a return item context (legacy)."""
        expr = self.visit(ctx.oC_Expression())

        if not isinstance(expr, QueryExpression):
            return None

        # Get alias if present
        alias = ""
        if ctx.oC_Variable():
            alias = ctx.oC_Variable().getText()
        elif isinstance(expr, QueryExpressionProperty):
            alias = expr.property_name or expr.variable_name
        else:
            alias = str(expr)

        return QueryExpressionWithAlias(inner_expression=expr, alias=alias)

    def visit_oC_Order(self, ctx: Any) -> list[SortItem]:
        """Visit an ORDER BY clause context."""
        items: list[SortItem] = []

        for sort_ctx in ctx.oC_SortItem() or []:
            item = self.visit(sort_ctx)
            if isinstance(item, SortItem):
                items.append(item)

        return items

    def visit_oC_SortItem(self, ctx: Any) -> SortItem:
        """Visit a sort item context."""
        expr = self.visit(ctx.oC_Expression())

        # Check for DESC
        text_children = [
            ctx.getChild(i).getText()
            for i in range(ctx.getChildCount())
            if hasattr(ctx.getChild(i), "getText")
        ]
        order = (
            SortOrder.DESC
            if any(t.upper() in ("DESC", "DESCENDING") for t in text_children)
            else SortOrder.ASC
        )

        if not isinstance(expr, QueryExpression):
            expr = QueryExpressionValue(value=None, value_type=type(None))

        return SortItem(expression=expr, order=order)

    def visit_oC_Limit(self, ctx: Any) -> QueryExpression:
        """Visit a LIMIT clause context."""
        return self.visit(ctx.oC_Expression())

    def visit_oC_Skip(self, ctx: Any) -> QueryExpression:
        """Visit a SKIP clause context."""
        return self.visit(ctx.oC_Expression())

    # =========================================================================
    # Expression visitors
    # =========================================================================

    def visit_oC_Expression(self, ctx: Any) -> QueryExpression:
        """Visit an expression context."""
        return self.visit(ctx.oC_OrExpression())

    def visit_oC_OrExpression(self, ctx: Any) -> QueryExpression:
        """Visit an OR expression context."""
        return self._visit_binary_expression(ctx, ctx.oC_XorExpression(), "or")

    def visit_oC_XorExpression(self, ctx: Any) -> QueryExpression:
        """Visit an XOR expression context."""
        return self._visit_binary_expression(ctx, ctx.oC_AndExpression(), "xor")

    def visit_oC_AndExpression(self, ctx: Any) -> QueryExpression:
        """Visit an AND expression context."""
        return self._visit_binary_expression(ctx, ctx.oC_NotExpression(), "and")

    def visit_oC_NotExpression(self, ctx: Any) -> QueryExpression:
        """Visit a NOT expression context."""
        # Check if NOT is present
        text = ctx.getText()
        has_not = text.upper().startswith("NOT")

        inner = self.visit(ctx.oC_ComparisonExpression())

        if has_not and isinstance(inner, QueryExpression):
            return QueryExpressionFunction(
                function=Function.NOT,
                parameters=[inner],
                data_type=bool,
            )

        return inner

    def visit_oC_ComparisonExpression(self, ctx: Any) -> QueryExpression:
        """Visit a comparison expression context."""
        # New grammar uses oC_StringListNullPredicateExpression
        if hasattr(ctx, "oC_StringListNullPredicateExpression"):
            left = self.visit(ctx.oC_StringListNullPredicateExpression())
        else:
            # Fallback for old grammar
            left = self.visit(ctx.oC_AddOrSubtractExpression())

        # Handle partial comparison expressions
        if ctx.oC_PartialComparisonExpression():
            for partial_ctx in ctx.oC_PartialComparisonExpression():
                op_text = ""
                for i in range(partial_ctx.getChildCount()):
                    child = partial_ctx.getChild(i)
                    if not hasattr(child, "getRuleIndex"):
                        op_text = child.getText()
                        break

                # Get the right side from partial comparison
                if hasattr(partial_ctx, "oC_StringListNullPredicateExpression"):
                    right = self.visit(
                        partial_ctx.oC_StringListNullPredicateExpression()
                    )
                else:
                    right = self.visit(partial_ctx.oC_AddOrSubtractExpression())

                op_info = try_get_operator(op_text)

                if op_info and isinstance(right, QueryExpression):
                    left = QueryExpressionBinary(
                        operator=op_info,
                        left_expression=(
                            left if isinstance(left, QueryExpression) else None
                        ),
                        right_expression=right,
                    )

        return left

    def visit_oC_StringListNullPredicateExpression(
        self, ctx: Any
    ) -> QueryExpression:
        """Visit a string/list/null predicate expression context."""
        result = self.visit(ctx.oC_AddOrSubtractExpression())

        # Handle IS NULL, IS NOT NULL, IN, etc.
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if hasattr(child, "getRuleIndex"):
                child_name = child.__class__.__name__
                if "NullPredicateExpression" in child_name:
                    # IS NULL or IS NOT NULL
                    text = child.getText().upper()
                    if "NOT" in text:
                        result = QueryExpressionFunction(
                            function=Function.IS_NOT_NULL,
                            parameters=(
                                [result]
                                if isinstance(result, QueryExpression)
                                else []
                            ),
                            data_type=bool,
                        )
                    else:
                        result = QueryExpressionFunction(
                            function=Function.IS_NULL,
                            parameters=(
                                [result]
                                if isinstance(result, QueryExpression)
                                else []
                            ),
                            data_type=bool,
                        )

        return result

    def visit_oC_AddOrSubtractExpression(self, ctx: Any) -> QueryExpression:
        """Visit an add/subtract expression context."""
        return self._visit_chained_binary_expression(
            ctx, ctx.oC_MultiplyDivideModuloExpression(), ["+", "-"]
        )

    def visit_oC_MultiplyDivideModuloExpression(self, ctx: Any) -> QueryExpression:
        """Visit a multiply/divide/modulo expression context."""
        return self._visit_chained_binary_expression(
            ctx, ctx.oC_PowerOfExpression(), ["*", "/", "%"]
        )

    def visit_oC_PowerOfExpression(self, ctx: Any) -> QueryExpression:
        """Visit a power expression context."""
        return self._visit_chained_binary_expression(
            ctx, ctx.oC_UnaryAddOrSubtractExpression(), ["^"]
        )

    def visit_oC_UnaryAddOrSubtractExpression(self, ctx: Any) -> QueryExpression:
        """Visit a unary add/subtract expression context."""
        # New grammar uses oC_NonArithmeticOperatorExpression
        if hasattr(ctx, "oC_NonArithmeticOperatorExpression"):
            inner = self.visit(ctx.oC_NonArithmeticOperatorExpression())
        # Old grammar uses oC_StringListNullOperatorExpression
        elif hasattr(ctx, "oC_StringListNullOperatorExpression"):
            inner = self.visit(ctx.oC_StringListNullOperatorExpression())
        else:
            inner = self.visit_children(ctx)

        # Check for unary operator
        text = ctx.getText()
        if text.startswith("-") and isinstance(inner, QueryExpression):
            return QueryExpressionFunction(
                function=Function.NEGATIVE,
                parameters=[inner],
            )

        return inner

    def visit_oC_NonArithmeticOperatorExpression(
        self, ctx: Any
    ) -> QueryExpression:
        """Visit a non-arithmetic operator expression context (new grammar)."""
        # This goes to oC_Atom directly in new grammar
        result = self.visit(ctx.oC_Atom())

        # Handle property lookups
        if hasattr(ctx, "oC_PropertyLookup") and ctx.oC_PropertyLookup():
            for prop_ctx in ctx.oC_PropertyLookup():
                prop_name = self.visit(prop_ctx)
                if isinstance(result, QueryExpressionProperty) and prop_name:
                    result.property_name = prop_name

        return result

    def visit_oC_PropertyLookup(self, ctx: Any) -> str | None:
        """Visit a property lookup context."""
        if ctx.oC_PropertyKeyName():
            return ctx.oC_PropertyKeyName().getText()
        return None

    def visit_oC_StringListNullOperatorExpression(
        self, ctx: Any
    ) -> QueryExpression:
        """Visit a string/list/null operator expression context (old grammar)."""
        result = self.visit(ctx.oC_PropertyOrLabelsExpression())

        # Handle IS NULL / IS NOT NULL
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if hasattr(child, "getText"):
                text = child.getText().upper()
                if "IS" in text and "NULL" in text:
                    if "NOT" in text:
                        result = QueryExpressionFunction(
                            function=Function.IS_NOT_NULL,
                            parameters=[result] if isinstance(result, QueryExpression) else [],
                        )
                    else:
                        result = QueryExpressionFunction(
                            function=Function.IS_NULL,
                            parameters=[result] if isinstance(result, QueryExpression) else [],
                        )

        return result

    def visit_oC_PropertyOrLabelsExpression(self, ctx: Any) -> QueryExpression:
        """Visit a property or labels expression context."""
        result = self.visit(ctx.oC_Atom())

        # Handle property lookups
        for lookup_ctx in ctx.oC_PropertyLookup() or []:
            prop_name = lookup_ctx.oC_PropertyKeyName().getText()
            if isinstance(result, QueryExpressionProperty):
                result.property_name = prop_name
            elif isinstance(result, QueryExpression):
                result = QueryExpressionProperty(
                    variable_name=str(result),
                    property_name=prop_name,
                )

        return result

    # =========================================================================
    # Atom visitors
    # =========================================================================

    def visit_oC_Atom(self, ctx: Any) -> QueryExpression:
        """Visit an atom context."""
        if ctx.oC_Literal():
            return self.visit(ctx.oC_Literal())
        if ctx.oC_Variable():
            return QueryExpressionProperty(variable_name=ctx.oC_Variable().getText())
        if ctx.oC_FunctionInvocation():
            return self.visit(ctx.oC_FunctionInvocation())
        if ctx.oC_ParenthesizedExpression():
            return self.visit(ctx.oC_ParenthesizedExpression())
        if ctx.oC_CaseExpression():
            return self.visit(ctx.oC_CaseExpression())
        if ctx.oC_ListComprehension():
            return self.visit(ctx.oC_ListComprehension())
        if ctx.oC_COUNT():
            return QueryExpressionAggregationFunction(
                aggregation_function=try_parse_aggregation_function("count")
                or AggregationFunction.COUNT,
            )

        # Default: return a property with the text
        return QueryExpressionProperty(variable_name=ctx.getText())

    def visit_oC_Literal(self, ctx: Any) -> QueryExpressionValue:
        """Visit a literal context."""
        if ctx.oC_NumberLiteral():
            return self.visit(ctx.oC_NumberLiteral())
        if ctx.StringLiteral():
            text = ctx.StringLiteral().getText()
            # Remove quotes
            if (text.startswith("'") and text.endswith("'")) or (
                text.startswith('"') and text.endswith('"')
            ):
                text = text[1:-1]
            return QueryExpressionValue(value=text, value_type=str)
        if ctx.oC_BooleanLiteral():
            text = ctx.oC_BooleanLiteral().getText().lower()
            return QueryExpressionValue(value=text == "true", value_type=bool)
        if ctx.getText().upper() == "NULL":
            return QueryExpressionValue(value=None, value_type=type(None))
        if ctx.oC_ListLiteral():
            return self.visit(ctx.oC_ListLiteral())
        if ctx.oC_MapLiteral():
            return self.visit(ctx.oC_MapLiteral())

        return QueryExpressionValue(value=ctx.getText(), value_type=str)

    def visit_oC_NumberLiteral(self, ctx: Any) -> QueryExpressionValue:
        """Visit a number literal context."""
        if ctx.oC_IntegerLiteral():
            text = ctx.oC_IntegerLiteral().getText()
            value = int(text)
            return QueryExpressionValue(value=value, value_type=int)
        if ctx.oC_DoubleLiteral():
            text = ctx.oC_DoubleLiteral().getText()
            value = float(text)
            return QueryExpressionValue(value=value, value_type=float)

        return QueryExpressionValue(value=0, value_type=int)

    def visit_oC_ListLiteral(self, ctx: Any) -> QueryExpressionList:
        """Visit a list literal context."""
        items: list[QueryExpression] = []
        for expr_ctx in ctx.oC_Expression() or []:
            expr = self.visit(expr_ctx)
            if isinstance(expr, QueryExpression):
                items.append(expr)
        return QueryExpressionList(items=items)

    def visit_oC_FunctionInvocation(self, ctx: Any) -> QueryExpression:
        """Visit a function invocation context."""
        func_name = ctx.oC_FunctionName().getText()

        # Check for DISTINCT
        is_distinct = any(
            ctx.getChild(i).getText().upper() == "DISTINCT"
            for i in range(ctx.getChildCount())
            if hasattr(ctx.getChild(i), "getText")
        )

        # Get parameters
        params: list[QueryExpression] = []
        for expr_ctx in ctx.oC_Expression() or []:
            expr = self.visit(expr_ctx)
            if isinstance(expr, QueryExpression):
                params.append(expr)

        # Check if it's an aggregation function
        agg_func = try_parse_aggregation_function(func_name)
        if agg_func:
            return QueryExpressionAggregationFunction(
                aggregation_function=agg_func,
                is_distinct=is_distinct,
                inner_expression=params[0] if params else None,
            )

        # Check if it's a regular function
        func_info = try_get_function(func_name)
        if func_info:
            return QueryExpressionFunction(
                function=func_info.function_name,
                parameters=params,
            )

        # Unknown function - return as generic function
        return QueryExpressionFunction(
            function=Function.INVALID,
            parameters=params,
        )

    def visit_oC_ParenthesizedExpression(self, ctx: Any) -> QueryExpression:
        """Visit a parenthesized expression context."""
        return self.visit(ctx.oC_Expression())

    def visit_oC_CaseExpression(self, ctx: Any) -> QueryExpressionCaseExpression:
        """Visit a CASE expression context."""
        test_expr = None
        alternatives: list[tuple[QueryExpression, QueryExpression]] = []
        else_expr = None

        # Get test expression if present
        exprs = ctx.oC_Expression() or []

        # Process WHEN/THEN pairs
        for i in range(0, len(exprs) - 1, 2):
            when_expr = self.visit(exprs[i])
            then_expr = self.visit(exprs[i + 1])
            if isinstance(when_expr, QueryExpression) and isinstance(then_expr, QueryExpression):
                alternatives.append((when_expr, then_expr))

        # Get ELSE expression if present
        if len(exprs) % 2 == 1:
            else_expr = self.visit(exprs[-1])
            if not isinstance(else_expr, QueryExpression):
                else_expr = None

        return QueryExpressionCaseExpression(
            test_expression=test_expr,
            alternatives=alternatives,
            else_expression=else_expr,
        )

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _visit_binary_expression(
        self,
        ctx: Any,
        sub_expressions: list[Any],
        operator_keyword: str,
    ) -> QueryExpression:
        """Visit a binary expression with a single operator type."""
        if not sub_expressions:
            return QueryExpressionValue(value=None, value_type=type(None))

        result = self.visit(sub_expressions[0])

        if len(sub_expressions) > 1:
            op_info = try_get_operator(operator_keyword)
            for sub_expr in sub_expressions[1:]:
                right = self.visit(sub_expr)
                if isinstance(right, QueryExpression) and isinstance(result, QueryExpression):
                    result = QueryExpressionBinary(
                        operator=op_info,
                        left_expression=result,
                        right_expression=right,
                    )

        return result

    def _visit_chained_binary_expression(
        self,
        ctx: Any,
        sub_expressions: list[Any],
        operators: list[str],
    ) -> QueryExpression:
        """Visit a chained binary expression with multiple operator types."""
        if not sub_expressions:
            return QueryExpressionValue(value=None, value_type=type(None))

        result = self.visit(sub_expressions[0])

        # Find operators in the children
        op_index = 0
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if not hasattr(child, "getRuleIndex"):
                text = child.getText()
                if text in operators and op_index < len(sub_expressions) - 1:
                    op_index += 1
                    right = self.visit(sub_expressions[op_index])
                    op_info = try_get_operator(text)
                    if isinstance(right, QueryExpression) and isinstance(result, QueryExpression):
                        result = QueryExpressionBinary(
                            operator=op_info,
                            left_expression=result,
                            right_expression=right,
                        )

        # If we didn't find operators in children, just visit the rest
        if op_index == 0:
            for i, sub_expr in enumerate(sub_expressions[1:], 1):
                right = self.visit(sub_expr)
                if isinstance(right, QueryExpression) and isinstance(result, QueryExpression):
                    result = QueryExpressionBinary(
                        operator=try_get_operator(operators[0] if operators else "+"),
                        left_expression=result,
                        right_expression=right,
                    )

        return result
