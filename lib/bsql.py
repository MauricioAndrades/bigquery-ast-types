"""
BigQuery SQL Transformation Wrapper

Ergonomic wrapper around sqlglot for AST-based SQL transformations.
Provides jQuery-like Iterator pattern for traversing and transforming SQL.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""
from typing import (
    Any,
    Callable,
    Optional,
    List,
    Union,
    Iterator as PyIterator,
    TypeVar,
    Generic,
)
from dataclasses import dataclass
import sqlglot
from sqlglot import exp


T = TypeVar("T", bound=exp.Expression)


class Iterator(Generic[T]):
    """jQuery-like iterator for SQL AST nodes with type safety."""

    def __init__(
        self,
        node: exp.Expression,
        node_type: Optional[type[T]] = None,
        predicate: Optional[Callable] = None,
    ):
        self.node = node
        self.node_type = node_type
        self.predicate = predicate

    def __iter__(self) -> PyIterator["SQLNode"]:
        # Use sqlglot's walk() for traversal as recommended
        for n in self.node.walk():
            if (self.node_type is None or isinstance(n, self.node_type)) and (
                self.predicate is None or self.predicate(SQLNode(n))
            ):
                yield SQLNode(n)

    def map(self, fn: Callable[["SQLNode"], Any]) -> List[Any]:
        """Map function over matching nodes."""
        return [fn(n) for n in self]

    def filter(self, fn: Callable[["SQLNode"], bool]) -> "Iterator":
        """Filter nodes by additional predicate."""
        combined_predicate = lambda n: (
            self.predicate(n) if self.predicate else True
        ) and fn(n)
        return Iterator(self.node, self.node_type, combined_predicate)

    def replaceWith(
        self, fn: Union[Callable[["SQLNode"], exp.Expression], exp.Expression]
    ) -> "Iterator":
        """Replace all matching nodes."""
        # Collect nodes first to avoid mutation during iteration
        nodes = list(self)
        for n in nodes:
            if callable(fn):
                n.replaceWith(fn(n))
            else:
                n.replaceWith(fn)
        return self

    def forEach(self, fn: Callable[["SQLNode", int], None]) -> "Iterator":
        """Execute function for each node."""
        for i, n in enumerate(self):
            fn(n, i)
        return self

    def first(self) -> Optional["SQLNode"]:
        """Get first matching node."""
        for n in self:
            return n
        return None

    def toList(self) -> List["SQLNode"]:
        """Convert to list."""
        return list(self)

    def count(self) -> int:
        """Count matching nodes."""
        return len(self.toList())


class SQLNode:
    """Wrapper for sqlglot Expression nodes."""

    def __init__(self, node: exp.Expression):
        self.node = node

    def find(
        self, node_type: type[T], predicate: Optional[Callable] = None
    ) -> Iterator[T]:
        """Find descendant nodes of given type."""
        return Iterator(self.node, node_type, predicate)

    def findAll(self, node_type: type[T]) -> List["SQLNode"]:
        """Find all descendant nodes of given type using sqlglot's find_all."""
        # Use sqlglot's native find_all for efficiency
        return [SQLNode(n) for n in self.node.find_all(node_type)]

    def parent(self) -> Optional["SQLNode"]:
        """Get parent node."""
        return SQLNode(self.node.parent) if self.node.parent else None

    def replaceWith(self, new_node: Union["SQLNode", exp.Expression]) -> "SQLNode":
        """Replace this node in the tree."""
        new_exp = new_node.node if isinstance(new_node, SQLNode) else new_node

        if self.node.parent:
            # sqlglot's replace method
            self.node.replace(new_exp)

        return SQLNode(new_exp)

    def remove(self) -> None:
        """Remove this node from tree."""
        if self.node.parent:
            self.node.pop()

    def sql(self, dialect: str = "bigquery", pretty: bool = False) -> str:
        """Convert to SQL string."""
        return self.node.sql(dialect=dialect, pretty=pretty)

    def transform(self, fn: Callable[[exp.Expression], exp.Expression]) -> "SQLNode":
        """Transform this node using sqlglot's transform method."""
        # Use sqlglot's native transform for deep transformations
        return SQLNode(self.node.transform(fn).copy())

    def __repr__(self) -> str:
        return f"<SQLNode {type(self.node).__name__}: {str(self.node)[:50]}...>"

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to underlying node."""
        return getattr(self.node, name)


class j:
    """Builder functions for SQL AST nodes."""

    @staticmethod
    def parse(sql: str, dialect: str = "bigquery") -> SQLNode:
        """Parse SQL string into AST."""
        return SQLNode(sqlglot.parse_one(sql, read=dialect))

    @staticmethod
    def parseMany(sql: str, dialect: str = "bigquery") -> List[SQLNode]:
        """Parse multiple SQL statements."""
        return [SQLNode(stmt) for stmt in sqlglot.parse(sql, read=dialect)]

    @staticmethod
    def Iterator(
        node: Union[SQLNode, exp.Expression],
        node_type: Optional[type] = None,
        predicate: Optional[Callable] = None,
    ) -> Iterator:
        """Create iterator for node."""
        exp_node = node.node if isinstance(node, SQLNode) else node
        return Iterator(exp_node, node_type, predicate)

    # Builders for common nodes
    @staticmethod
    def Select(*expressions: Union[str, exp.Expression, SQLNode], **kwargs) -> SQLNode:
        """Build SELECT statement."""
        exprs = []
        for e in expressions:
            if isinstance(e, str):
                exprs.append(exp.column(e))
            elif isinstance(e, SQLNode):
                exprs.append(e.node)
            elif isinstance(e, exp.Expression):
                exprs.append(e)
            else:
                raise TypeError(f"Unsupported type for Select: {type(e)}")
        return SQLNode(exp.Select(expressions=exprs, **kwargs))

    @staticmethod
    def Column(name: str, table: Optional[str] = None) -> SQLNode:
        """Build column reference."""
        if table:
            return SQLNode(exp.Column(this=name, table=table))
        return SQLNode(exp.Column(this=name))

    @staticmethod
    def Table(
        name: str, db: Optional[str] = None, catalog: Optional[str] = None
    ) -> SQLNode:
        """Build table reference."""
        return SQLNode(exp.Table(this=name, db=db, catalog=catalog))

    @staticmethod
    def Literal(value: Any) -> SQLNode:
        """Build literal value."""
        return SQLNode(exp.Literal.string(str(value)))

    @staticmethod
    def Null() -> SQLNode:
        """Build NULL literal."""
        return SQLNode(exp.Null())

    @staticmethod
    def Case(*whens, default: Optional[exp.Expression] = None) -> SQLNode:
        """Build CASE expression."""
        case_exp = exp.Case(ifs=[], default=default)
        for when in whens:
            if isinstance(when, tuple) and len(when) == 2:
                condition, result = when
                case_exp.args["ifs"].append(exp.If(this=condition, true=result))
        return SQLNode(case_exp)

    @staticmethod
    def When(condition: exp.Expression, result: exp.Expression) -> tuple:
        """Build WHEN clause for CASE."""
        return (condition, result)

    @staticmethod
    def And(*conditions: exp.Expression) -> SQLNode:
        """Build AND expression."""
        if not conditions:
            raise ValueError("And requires at least one condition")
        result = conditions[0]
        for cond in conditions[1:]:
            result = exp.And(this=result, expression=cond)
        return SQLNode(result)

    @staticmethod
    def Or(*conditions: exp.Expression) -> SQLNode:
        """Build OR expression."""
        if not conditions:
            raise ValueError("Or requires at least one condition")
        result = conditions[0]
        for cond in conditions[1:]:
            result = exp.Or(this=result, expression=cond)
        return SQLNode(result)

    @staticmethod
    def Eq(left: exp.Expression, right: exp.Expression) -> SQLNode:
        """Build equality comparison."""
        return SQLNode(exp.EQ(this=left, expression=right))

    @staticmethod
    def Func(name: str, *args: exp.Expression) -> SQLNode:
        """Build function call."""
        return SQLNode(exp.func(name, *args))

    @staticmethod
    def Cast(expr: exp.Expression, to: str, safe: bool = False) -> SQLNode:
        """Build CAST expression."""
        if safe:
            return SQLNode(exp.Cast(this=expr, to=exp.DataType.build(to), safe=True))
        return SQLNode(exp.Cast(this=expr, to=exp.DataType.build(to)))

    @staticmethod
    def Alias(expr: Union[str, exp.Expression], alias: str) -> SQLNode:
        """Build aliased expression."""
        if isinstance(expr, str):
            expr = sqlglot.parse_one(expr, read="bigquery")
        elif isinstance(expr, SQLNode):
            expr = expr.node
        return SQLNode(exp.alias_(expr, alias))

    @staticmethod
    def CTE(name: str, query: Union[str, exp.Expression, SQLNode]) -> SQLNode:
        """Build CTE."""
        if isinstance(query, str):
            query = sqlglot.parse_one(query, read="bigquery")
        elif isinstance(query, SQLNode):
            query = query.node
        # Create a table alias for the CTE
        cte = exp.CTE(this=query, alias=exp.TableAlias(this=exp.Identifier(this=name)))
        return SQLNode(cte)

    @staticmethod
    def Window(
        func: exp.Expression,
        partition_by: Optional[List[exp.Expression]] = None,
        order_by: Optional[List[exp.Expression]] = None,
    ) -> SQLNode:
        """Build window function."""
        window_spec = exp.WindowSpec(
            partition_by=partition_by or [], order_by=order_by or []
        )
        return SQLNode(exp.Window(this=func, window=window_spec))

    # Convenience method for building expressions
    def __new__(cls, expr: Union[str, exp.Expression], alias: Optional[str] = None):
        """Convenience builder: j("expr", "alias")."""
        if isinstance(expr, str):
            expr = sqlglot.parse_one(expr, read="bigquery")

        if alias is not None:
            return SQLNode(exp.alias_(expr, alias))

        return SQLNode(expr)


@dataclass
class Pattern:
    """Pattern matching helper for AST nodes."""

    node_type: type[exp.Expression]
    conditions: List[Callable[[exp.Expression], bool]]

    def matches(self, node: exp.Expression) -> bool:
        """Check if node matches this pattern."""
        if not isinstance(node, self.node_type):
            return False
        return all(cond(node) for cond in self.conditions)


class PatternMatcher:
    """Advanced pattern matching for AST transformations."""

    @staticmethod
    def match_case_when_null_to_default(node: exp.Expression) -> bool:
        """Match CASE WHEN col IS NULL THEN default patterns."""
        if not isinstance(node, exp.Case):
            return False

        # Check for single WHEN IS NULL pattern
        if len(node.args.get("ifs", [])) == 1:
            if_clause = node.args["ifs"][0]
            condition = if_clause.args.get("this")
            if isinstance(condition, exp.Is) and isinstance(
                condition.expression, exp.Null
            ):
                return True
        return False

    @staticmethod
    def match_string_comparison_pattern(node: exp.Expression) -> bool:
        """Match patterns like col = 'value' or col != ''."""
        if isinstance(node, (exp.EQ, exp.NEQ)):
            left = node.this
            right = node.expression
            return isinstance(left, exp.Column) and isinstance(right, exp.Literal)
        return False


# Transformation helpers
def null_safe_eq(left: exp.Expression, right: exp.Expression) -> exp.Expression:
    """Build null-safe equality: (A = B OR (A IS NULL AND B IS NULL))."""
    return exp.Paren(
        this=exp.Or(
            this=exp.EQ(this=left, expression=right),
            expression=exp.And(
                this=exp.Is(this=left, expression=exp.Null()),
                expression=exp.Is(this=right, expression=exp.Null()),
            ),
        )
    )


def standardize_string_id(col: exp.Expression) -> exp.Expression:
    """Standardize string ID: NULLIF(TRIM(col), '')."""
    return exp.func("NULLIF", exp.func("TRIM", col), exp.Literal.string(""))


def standardize_numeric(col: exp.Expression) -> exp.Expression:
    """Standardize numeric: COALESCE(SAFE_CAST(col AS NUMERIC), 0)."""
    return exp.func(
        "COALESCE",
        exp.Cast(this=col, to=exp.DataType.build("NUMERIC"), safe=True),
        exp.Literal.number(0),
    )


def standardize_boolean(col: exp.Expression) -> exp.Expression:
    """Standardize boolean: COALESCE(col, FALSE)."""
    return exp.func("COALESCE", col, exp.false())


def deep_copy_transform(
    node: exp.Expression,
    transform_fn: Callable[[exp.Expression], Optional[exp.Expression]],
) -> exp.Expression:
    """Deep copy and transform using sqlglot's native transform."""

    def transformer(n):
        result = transform_fn(n)
        return result if result is not None else n

    return node.copy().transform(transformer)


def extract_table_references(ast: exp.Expression) -> List[str]:
    """Extract all table references from AST."""
    tables = []
    for table in ast.find_all(exp.Table):
        if table.catalog:
            tables.append(f"{table.catalog}.{table.db}.{table.name}")
        elif table.db:
            tables.append(f"{table.db}.{table.name}")
        else:
            tables.append(table.name)
    return tables


def inject_cte(
    ast: exp.Expression, cte_name: str, cte_query: exp.Expression
) -> exp.Expression:
    """Inject a CTE into a query."""
    if not isinstance(ast, exp.Select):
        return ast

    # Copy to avoid mutation
    ast = ast.copy()

    # Create or update WITH clause
    new_cte = exp.CTE(
        this=cte_query,
        alias=exp.TableAlias(this=exp.Identifier(this=cte_name))
    )
    if ast.args.get("with"):
        # Add to existing WITH clause
        ast.args["with"].expressions.append(new_cte)
    else:
        # Create new WITH clause
        ast.args["with"] = exp.With(expressions=[new_cte])
    return ast
