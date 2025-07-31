#!/usr/bin/env python3
"""
Order Merge Transformation Demo

Demonstrates how developers can write simple SQL queries
and transform them into standardized 3-CTE merge patterns.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys

sys.path.append("..")

from alib import OrderMergeBuilder
from jsql import j


def demo_simple_to_complex():
    """Show transformation from simple query to complex merge."""
    print("=== Simple Query → Complex Merge Pattern ===")
    print()

    # Developer writes this simple query
    simple_sql = """
    SELECT 
        order_id,
        product_id,
        visitor_id,
        order_ts,
        quantity,
        price
    FROM `my-project.raw_data.orders`
    WHERE order_date >= '2024-01-01'
    """

    print("Developer writes:")
    print(simple_sql)
    print("\n" + "=" * 60 + "\n")

    # We transform it into standardized merge
    builder = OrderMergeBuilder("my-project", "event_analytics")
    complex_merge = builder.build_3cte_merge(
        source_sql=simple_sql,
        retailer_id=12345,
        datetime_threshold="2024-01-01 00:00:00",
    )

    print("We transform it to:")
    print(complex_merge)


def demo_chewy_full_outer_join():
    """Show Chewy's FULL OUTER JOIN pattern."""
    print("\n\n=== Chewy FULL OUTER JOIN Transformation ===")
    print()

    # Developer indicates they want Chewy pattern
    from alib import RetailerPatterns

    chewy_source = RetailerPatterns.chewy_full_outer_join("2024-01-01 00:00:00")

    print("Chewy's FULL OUTER JOIN source:")
    print(chewy_source)
    print("\n" + "=" * 60 + "\n")

    # Transform into standardized merge
    builder = OrderMergeBuilder("symbiosys-prod", "event_analytics")
    chewy_merge = builder.build_3cte_merge(
        source_sql=chewy_source,
        retailer_id=1001,  # Chewy's retailer ID
        datetime_threshold="2024-01-01 00:00:00",
    )

    print("Transformed to standardized merge:")
    print(chewy_merge)


def demo_ast_manipulation():
    """Show direct AST manipulation using jsql."""
    print("\n\n=== Direct AST Manipulation ===")
    print()

    # Parse a query
    query = j.parse(
        """
    SELECT 
        order_id,
        product_id,
        quantity * price AS total
    FROM orders
    WHERE status = 'COMPLETED'
    """
    )

    print("Original query:")
    print(query.sql(pretty=True))
    print()

    # Add visitor_id standardization to all string columns
    query.find(exp.Column).forEach(lambda node, i: print(f"Found column: {node.name}"))

    # Transform string ID columns
    string_id_cols = ["order_id", "product_id"]

    def transform_string_ids(node):
        if node.name in string_id_cols:
            # Wrap in NULLIF(TRIM(col), '')
            return exp.func(
                "NULLIF", exp.func("TRIM", node.node), exp.Literal.string("")
            )
        return node.node

    query.find(exp.Column, predicate=lambda n: n.name in string_id_cols).replaceWith(
        transform_string_ids
    )

    print("\nAfter standardizing string IDs:")
    print(query.sql(pretty=True))


def demo_dedup_patterns():
    """Show different deduplication patterns."""
    print("\n\n=== Deduplication Patterns ===")
    print()

    from alib import DedupPatterns

    # Simple dedup
    simple = DedupPatterns.simple_dedup(
        partition_by=["retailer_id", "order_id", "product_id"],
        order_by=[("order_ts", "DESC"), ("symbiosys_ts", "DESC")],
    )

    print("Simple deduplication:")
    print(simple)

    # Comprehensive dedup (partitioning by ALL columns)
    all_columns = [
        "retailer_id",
        "order_id",
        "product_id",
        "visitor_id",
        "order_ts",
        "session_id",
        "quantity",
        "price",
        "child_product_id",
        "hashed_pii",
    ]

    comprehensive = DedupPatterns.comprehensive_dedup(all_columns)

    print("\nComprehensive deduplication:")
    print(comprehensive)


def demo_standardization():
    """Show column standardization patterns."""
    print("\n\n=== Column Standardization ===")
    print()

    from alib import StandardizationPatterns

    # Define column types
    columns = {
        "visitor_id": "string_id",
        "order_id": "string_id",
        "quantity": "integer",
        "price": "numeric",
        "is_restaurant": "boolean",
    }

    # Generate standardized expressions
    standardized = StandardizationPatterns.standardize_columns(columns)

    # Show the SQL for each
    for expr in standardized:
        print(f"{expr.alias}: {expr.sql()}")


if __name__ == "__main__":
    demo_simple_to_complex()
    demo_chewy_full_outer_join()
    demo_ast_manipulation()
    demo_dedup_patterns()
    demo_standardization()

    print("\n\n🐕 Woof! Order merge demo complete!")
