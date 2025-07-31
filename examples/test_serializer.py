"""
Test the SQL Serializer

Demonstrates converting AST nodes back to formatted SQL.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import bigquery_ast_types as ast
from bigquery_ast_types import (
    b,
    to_sql,
    pretty_print,
    compact_print,
    SerializerOptions,
    Select,
    SelectColumn,
    TableRef,
    TableName,
    Identifier,
)


def test_simple_select():
    """Test serializing a simple SELECT statement."""
    print("=== Simple SELECT ===")

    # Build AST using builders
    query = b.select(
        b.col("order_id"),
        SelectColumn(b.col("customer_id"), "visitor_id"),
        SelectColumn(b.func("COUNT", b.col("*")), "order_count"),
    )
    
    # Set from clause
    table = TableRef(
        TableName(
            project=Identifier("my-project"),
            dataset=Identifier("my_dataset"),
            table=Identifier("orders"),
        )
    )
    query.from_clause = table

    # Test different formatting options
    print("\nExpanded format:")
    print(pretty_print(query))

    print("\nCompact format:")
    print(compact_print(query))

    # Custom options
    print("\nCustom format (lowercase keywords):")
    custom_options = SerializerOptions(
        uppercase_keywords=False, format_style="expanded"
    )
    print(to_sql(query, custom_options))


def test_literals():
    """Test serializing different literal types."""
    print("\n\n=== Literals ===")
    
    query = b.select(
        SelectColumn(b.lit("Hello World"), "string_val"),
        SelectColumn(b.lit(42), "int_val"),
        SelectColumn(b.lit(3.14), "float_val"),
        SelectColumn(b.lit(True), "bool_val"),
        SelectColumn(b.lit(None), "null_val"),
    )
    
    print(pretty_print(query))


if __name__ == "__main__":
    test_simple_select()
    test_literals()
    print("\n🎉 Serializer tests completed!")