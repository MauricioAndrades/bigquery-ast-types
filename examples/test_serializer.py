"""
Test the SQL Serializer

Demonstrates converting AST nodes back to formatted SQL.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
sys.path.append('..')

from ast.builders import b
from ast.serializer import to_sql, pretty_print, compact_print, SerializerOptions
from ast.ast_types import (
    SelectStatement, SelectItem, FromClause, TableReference,
    TableName, Identifier, WithClause, CTE, JoinExpression, JoinType,
    OrderByItem, OrderDirection
)


def test_simple_select():
    """Test serializing a simple SELECT statement."""
    print("=== Simple SELECT ===")

    # Build AST using builders
    query = SelectStatement(
        select_list=[
            SelectItem(b.col('order_id')),
            SelectItem(b.col('customer_id'), Identifier('visitor_id')),
            SelectItem(
                b.func('COUNT', b.star()),
                Identifier('order_count')
            )
        ],
        from_clause=FromClause([
            TableReference(
                TableName(
                    project=Identifier('my-project'),
                    dataset=Identifier('my_dataset'),
                    table=Identifier('orders')
                )
            )
        ]),
        where=b.and_(
            b.gte(b.col('order_date'), b.date('2024-01-01')),
            b.eq(b.col('status'), b.lit('COMPLETED'))
        )
    )

    # Test different formatting options
    print("\nExpanded format:")
    print(pretty_print(query))

    print("\nCompact format:")
    print(compact_print(query))

    # Custom options
    print("\nCustom format (lowercase keywords, no quotes):")
    custom_options = SerializerOptions(
        uppercase_keywords=False,
        quote_identifiers=False,
        format_style='expanded'
    )
    print(to_sql(query, custom_options))


def test_complex_merge():
    """Test serializing a complex MERGE statement."""
    print("\n\n=== Complex MERGE ===")

    # Build a MERGE with CTEs
    from ast.ast_types import (
        MergeStatement, MergeWhenClause, MergeInsert, MergeUpdate,
        MergeDelete, SetClause, SubqueryTable
    )

    # CTE for source data
    source_cte = CTE(
        name=Identifier('source_data'),
        query=SelectStatement(
            select_list=[
                SelectItem(b.col('order_id')),
                SelectItem(b.col('product_id')),
                SelectItem(b.col('quantity')),
                SelectItem(b.current_timestamp(), Identifier('updated_at'))
            ],
            from_clause=FromClause([
                TableReference(TableName(table=Identifier('staging_orders')))
            ]),
            where=b.gte(b.col('order_ts'), b.timestamp('2024-01-01 00:00:00'))
        )
    )

    # Build MERGE statement
    merge = MergeStatement(
        target_table=TableName(
            project=Identifier('my-project'),
            dataset=Identifier('warehouse'),
            table=Identifier('orders')
        ),
        source=SubqueryTable(
            SelectStatement(
                select_list=[SelectItem(b.star())],
                from_clause=FromClause([
                    TableReference(TableName(table=Identifier('source_data')))
                ])
            )
        ),
        on_condition=b.and_(
            b.eq(b.col('order_id', 'S'), b.col('order_id', 'T')),
            b.eq(b.col('product_id', 'S'), b.col('product_id', 'T'))
        ),
        when_clauses=[
            MergeWhenClause(
                match_type='MATCHED',
                action=MergeUpdate([
                    SetClause(Identifier('quantity'), b.col('quantity', 'S')),
                    SetClause(Identifier('updated_at'), b.col('updated_at', 'S'))
                ])
            ),
            MergeWhenClause(
                match_type='NOT MATCHED BY TARGET',
                action=MergeInsert(row=True)
            ),
            MergeWhenClause(
                match_type='NOT MATCHED BY SOURCE',
                condition=b.lt(b.col('updated_at', 'T'), b.timestamp('2024-01-01')),
                action=MergeDelete()
            )
        ]
    )

    # Create full query with CTE
    full_query = SelectStatement(
        with_clause=WithClause([source_cte]),
        select_list=[SelectItem(b.lit(1))]  # Dummy select for the merge
    )

    print("\nFormatted MERGE:")
    print(pretty_print(merge))


def test_window_functions():
    """Test serializing window functions."""
    print("\n\n=== Window Functions ===")

    from ast.ast_types import WindowFunction, WindowSpecification

    # Build query with window functions
    query = SelectStatement(
        select_list=[
            SelectItem(b.col('order_id')),
            SelectItem(b.col('customer_id')),
            SelectItem(b.col('order_total')),
            SelectItem(
                WindowFunction(
                    function=b.func('ROW_NUMBER'),
                    window=WindowSpecification(
                        partition_by=[b.col('customer_id')],
                        order_by=[
                            OrderByItem(b.col('order_date'), OrderDirection.DESC)
                        ]
                    )
                ),
                Identifier('order_rank')
            ),
            SelectItem(
                WindowFunction(
                    function=b.func('SUM', b.col('order_total')),
                    window=WindowSpecification(
                        partition_by=[b.col('customer_id')],
                        order_by=[
                            OrderByItem(b.col('order_date'), OrderDirection.ASC)
                        ]
                    )
                ),
                Identifier('running_total')
            )
        ],
        from_clause=FromClause([
            TableReference(TableName(table=Identifier('orders')))
        ])
    )

    print("\nWindow function query:")
    print(pretty_print(query))


def test_case_expressions():
    """Test serializing CASE expressions."""
    print("\n\n=== CASE Expressions ===")

    # Build complex CASE expression
    case_expr = b.case(
        b.when(b.gt(b.col('order_total'), b.lit(1000)), b.lit('HIGH')),
        b.when(b.gt(b.col('order_total'), b.lit(500)), b.lit('MEDIUM')),
        b.when(b.gt(b.col('order_total'), b.lit(0)), b.lit('LOW')),
        else_=b.lit('ZERO')
    )

    query = SelectStatement(
        select_list=[
            SelectItem(b.col('order_id')),
            SelectItem(case_expr, Identifier('order_category'))
        ],
        from_clause=FromClause([
            TableReference(TableName(table=Identifier('orders')))
        ])
    )

    print("\nCASE expression query:")
    print(pretty_print(query))


if __name__ == "__main__":
    test_simple_select()
    test_complex_merge()
    test_window_functions()
    test_case_expressions()

    print("\n\n🐕 Woof! SQL serialization complete!")