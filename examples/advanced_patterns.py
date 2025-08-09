#!/usr/bin/env python3
"""
Advanced AST Pattern Examples

Demonstrates advanced sqlglot features discovered in research:
- Native find_all() usage
- transform() for deep mutations
- Pattern matching
- Multi-statement handling
- Table reference extraction

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys

# Add project and library paths if needed
sys.path.append("/Users/op/libs")
sys.path.append("/Users/op/Documents/sym/symbo/.context/bigquery-ast-types/lib")

import sqlglot
from sqlglot import exp

# Import custom AST utilities and pattern matchers
from lib.bsql import (
    j,
    SQLNode,
    Pattern,
    PatternMatcher,
    deep_copy_transform,
    extract_table_references,
    inject_cte,
)


def demo_native_sqlglot_features():
    """Show native sqlglot AST manipulation."""
    print("=== Native sqlglot Features ===")
    print()

    sql = """
    SELECT
        o.order_id,
        o.customer_id,
        p.product_name,
        o.quantity * p.price AS total
    FROM orders o
    JOIN products p ON o.product_id = p.id
    WHERE o.status = 'COMPLETED'
    """

    ast = sqlglot.parse_one(sql, read="bigquery")

    # Use native walk()
    print("All nodes via walk():")
    for i, node in enumerate(ast.walk()):
        if i < 10:  # First 10 nodes
            print(f"  {type(node).__name__}: {str(node)[:50]}...")

    print("\nTables via find_all():")
    for table in ast.find_all(exp.Table):
        print(f"  Table: {table.name}")

    print("\nColumns via find_all():")
    for col in ast.find_all(exp.Column):
        full_name = f"{col.table}.{col.name}" if col.table else col.name
        print(f"  Column: {full_name}")


def demo_deep_transform():
    """Show sqlglot's transform() for deep mutations."""
    print("\n\n=== Deep Transform with sqlglot ===")
    print()

    sql = """
    SELECT
        customer_id,
        CASE
            WHEN total > 1000 THEN 'HIGH'
            WHEN total > 500 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS category
    FROM orders
    """

    ast = sqlglot.parse_one(sql, read="bigquery")

    print("Original:")
    print(ast.sql(pretty=True))

    # Transform all numeric literals to parameters
    param_count = 0

    def parameterize_numbers(node):
        nonlocal param_count
        if isinstance(node, exp.Literal) and node.is_number:
            param_count += 1
            # Replace with parameter placeholder
            return exp.Placeholder(this=f"param_{param_count}")
        return node

    # Use sqlglot's native transform
    transformed = ast.transform(parameterize_numbers)

    print("\nAfter parameterization:")
    print(transformed.sql(pretty=True))
    print(f"\nExtracted {param_count} parameters")


def demo_pattern_matching():
    """Show pattern matching capabilities."""
    print("\n\n=== Pattern Matching ===")
    print()

    sql = """
    SELECT
        customer_id,
        CASE WHEN email IS NULL THEN 'unknown' ELSE email END AS email,
        CASE WHEN phone IS NULL THEN '' ELSE phone END AS phone,
        order_total
    FROM customers
    WHERE status = 'ACTIVE' AND country != ''
    """

    ast = sqlglot.parse_one(sql, read="bigquery")

    # Find CASE WHEN NULL patterns
    print("CASE WHEN NULL patterns found:")
    for node in ast.walk():
        if PatternMatcher.match_case_when_null_to_default(node):
            print(f"  {node.sql()}")

    # Find string comparisons
    print("\nString comparison patterns:")
    for node in ast.walk():
        if PatternMatcher.match_string_comparison_pattern(node):
            print(f"  {node.sql()}")

    # Transform CASE NULL to COALESCE
    def case_to_coalesce(node):
        if PatternMatcher.match_case_when_null_to_default(node):
            # Extract the column and default value
            if_clause = node.args["ifs"][0]
            condition = if_clause.this

            if isinstance(condition, exp.Is):
                col = condition.this
                default = if_clause.args["true"]
                else_val = node.args.get("default")

                # Replace with COALESCE
                return exp.func("COALESCE", col, else_val)

        return node

    transformed = ast.transform(case_to_coalesce)

    print("\nAfter CASE to COALESCE transformation:")
    print(transformed.sql(pretty=True))


def demo_multi_statement():
    """Show multi-statement SQL handling."""
    print("\n\n=== Multi-Statement SQL ===")
    print()

    script = """
    -- Create temp table
    CREATE TEMP TABLE order_summary AS
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(total) as total_spent
    FROM orders
    GROUP BY customer_id;

    -- Update customer stats
    UPDATE customers c
    SET
        total_orders = os.order_count,
        total_spent = os.total_spent
    FROM order_summary os
    WHERE c.id = os.customer_id;

    -- Select results
    SELECT * FROM customers WHERE total_orders > 10;
    """

    # Parse multiple statements
    statements = sqlglot.parse(script, read="bigquery")

    print(f"Found {len(statements)} statements:\n")

    for i, stmt in enumerate(statements, 1):
        stmt_type = type(stmt).__name__
        print(f"Statement {i} ({stmt_type}):")

        # Extract table references
        tables = extract_table_references(stmt) if stmt is not None else []
        if tables:
            print(f"  Tables: {', '.join(tables)}")

        # Show first line of SQL
        if stmt is not None:
            sql_lines = stmt.sql(pretty=True).split("\n")
            print(f"  SQL: {sql_lines[0]}...")
        else:
            print("  SQL: <None>")
        print()


def demo_cte_injection():
    """Show CTE injection pattern."""
    print("\n\n=== CTE Injection ===")
    print()

    original = """
    SELECT
        customer_id,
        SUM(total) as total_spent
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
    """

    ast = sqlglot.parse_one(original, read="bigquery")

    print("Original query:")
    print(ast.sql(pretty=True))

    # Create a CTE to inject
    filter_cte = sqlglot.parse_one(
        """
    SELECT DISTINCT customer_id
    FROM customers
    WHERE status = 'PREMIUM'
    """
    )

    # Inject the CTE
    with_cte = inject_cte(ast, "premium_customers", filter_cte)

    # Also modify the main query to use the CTE
    def add_join(node):
        if isinstance(node, exp.From):
            # Add JOIN to the CTE
            join = exp.Join(
                this=exp.Table(this="premium_customers", alias="pc"),
                on=exp.EQ(
                    this=exp.Column(this="customer_id", table="orders"),
                    expression=exp.Column(this="customer_id", table="pc"),
                ),
            )
            node.args.setdefault("joins", []).append(join)
        return node

    final = with_cte.transform(add_join)

    print("\nWith injected CTE and JOIN:")
    print(final.sql(pretty=True))


def demo_round_trip_fidelity():
    """Show round-trip serialization behavior."""
    print("\n\n=== Round-Trip Fidelity ===")
    print()

    original = """
    -- Important: Premium customer analysis
    SELECT
        c.customer_id,    -- Customer identifier
        c.name,
        COUNT(DISTINCT o.order_id) AS order_count,  -- Total orders

        /* Calculate total spent */
        SUM(o.total) AS total_spent

    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id

    WHERE
        c.status = 'ACTIVE'  -- Only active customers
        AND o.order_date >= '2024-01-01'

    GROUP BY 1, 2  -- Group by customer
    HAVING order_count > 0
    ORDER BY total_spent DESC
    """

    print("Original SQL (with comments and formatting):")
    print(original)

    # Parse and re-serialize
    ast = sqlglot.parse_one(original, read="bigquery")
    roundtrip = ast.sql(pretty=True, comments=False)  # Comments are lost

    print("\nAfter round-trip (comments lost, formatting changed):")
    print(roundtrip)

    print("\nNote: As per research, sqlglot doesn't preserve:")
    print("- Comments")
    print("- Original whitespace/formatting")
    print("- Token positions")
    print("This is a known limitation being discussed upstream.")


if __name__ == "__main__":
    demo_native_sqlglot_features()
    demo_deep_transform()
    demo_pattern_matching()
    demo_multi_statement()
    demo_cte_injection()
    demo_round_trip_fidelity()

    print("\n\n🐕 Woof! Advanced patterns demo complete!")
