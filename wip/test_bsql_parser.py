#!/usr/bin/env python3
"""
Test the bsql.py module with a real complex BigQuery query from Symbiosys
"""
import pprint
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from lib.bsql import j, Iterator, SQLNode, extract_table_references
from sqlglot import exp

# The complex query to parse
QUERY = """-- Compare orders between dev and prod accounting for duplicate submissions
-- The logs show orders can be submitted multiple times within milliseconds
-- We'll deduplicate orders within a 5-second window to get accurate counts

WITH prod_orders_deduped AS (
  -- Deduplicate prod orders within 5-second windows
  SELECT
    DATE(order_ts) as order_date,
    EXTRACT(HOUR FROM order_ts) as order_hour,
    order_id,
    MIN(order_ts) as first_order_ts,
    COUNT(*) as submission_count,
    MAX(order_ts) as last_order_ts,
    TIMESTAMP_DIFF(MAX(order_ts), MIN(order_ts), MILLISECOND) as submission_span_ms
  FROM `symbiosys-prod.event_api.order`
  WHERE order_ts >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
    AND order_ts < TIMESTAMP(CURRENT_DATE())
    AND retailer_id = 14  -- Macy's
  GROUP BY
    DATE(order_ts),
    EXTRACT(HOUR FROM order_ts),
    order_id
),

dev_orders_deduped AS (
  -- Deduplicate dev orders within 5-second windows
  SELECT
    DATE(order_ts) as order_date,
    EXTRACT(HOUR FROM order_ts) as order_hour,
    order_id,
    MIN(order_ts) as first_order_ts,
    COUNT(*) as submission_count,
    MAX(order_ts) as last_order_ts,
    TIMESTAMP_DIFF(MAX(order_ts), MIN(order_ts), MILLISECOND) as submission_span_ms
  FROM `symbiosys-dev.event_api.order`
  WHERE order_ts >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
    AND order_ts < TIMESTAMP(CURRENT_DATE())
    AND retailer_id = 14  -- Macy's
  GROUP BY
    DATE(order_ts),
    EXTRACT(HOUR FROM order_ts),
    order_id
),

-- Identify duplicate submissions (submitted more than once)
prod_duplicates AS (
  SELECT
    order_date,
    order_hour,
    COUNT(DISTINCT order_id) as orders_with_duplicates,
    SUM(submission_count) as total_submissions,
    SUM(submission_count) - COUNT(DISTINCT order_id) as extra_submissions,
    AVG(submission_count) as avg_submissions_per_order,
    MAX(submission_count) as max_submissions_per_order,
    COUNT(DISTINCT CASE WHEN submission_count > 1 THEN order_id END) as unique_orders_duplicated
  FROM prod_orders_deduped
  GROUP BY order_date, order_hour
),

dev_duplicates AS (
  SELECT
    order_date,
    order_hour,
    COUNT(DISTINCT order_id) as orders_with_duplicates,
    SUM(submission_count) as total_submissions,
    SUM(submission_count) - COUNT(DISTINCT order_id) as extra_submissions,
    AVG(submission_count) as avg_submissions_per_order,
    MAX(submission_count) as max_submissions_per_order,
    COUNT(DISTINCT CASE WHEN submission_count > 1 THEN order_id END) as unique_orders_duplicated
  FROM dev_orders_deduped
  GROUP BY order_date, order_hour
),

-- Compare hourly counts
hourly_comparison AS (
  SELECT
    COALESCE(p.order_date, d.order_date) as order_date,
    COALESCE(p.order_hour, d.order_hour) as order_hour,
    -- Raw submission counts
    COALESCE(p.total_submissions, 0) as prod_raw_count,
    COALESCE(d.total_submissions, 0) as dev_raw_count,
    -- Deduplicated counts (unique orders)
    COALESCE(p.orders_with_duplicates, 0) as prod_unique_orders,
    COALESCE(d.unique_orders_duplicated, 0) as dev_unique_orders,
    -- Duplication metrics
    COALESCE(p.extra_submissions, 0) as prod_extra_submissions,
    COALESCE(d.extra_submissions, 0) as dev_extra_submissions,
    COALESCE(p.avg_submissions_per_order, 1) as prod_avg_submissions,
    COALESCE(d.avg_submissions_per_order, 1) as dev_avg_submissions,
    COALESCE(p.max_submissions_per_order, 1) as prod_max_submissions,
    COALESCE(d.max_submissions_per_order, 1) as dev_max_submissions,
    -- Calculate differences
    COALESCE(d.total_submissions, 0) - COALESCE(p.total_submissions, 0) as raw_diff,
    COALESCE(d.unique_orders_duplicated, 0) - COALESCE(p.orders_with_duplicates, 0) as unique_diff
  FROM prod_duplicates p
  FULL OUTER JOIN dev_duplicates d
    ON p.order_date = d.order_date
    AND p.order_hour = d.order_hour
)

-- Final summary with deduplication analysis
SELECT
  order_date,
  order_hour,
  prod_raw_count,
  dev_raw_count,
  prod_unique_orders,
  dev_unique_orders,
  raw_diff,
  unique_diff,
  prod_extra_submissions,
  dev_extra_submissions,
  ROUND(prod_avg_submissions, 2) as prod_avg_submissions,
  ROUND(dev_avg_submissions, 2) as dev_avg_submissions,
  prod_max_submissions,
  dev_max_submissions,
  -- Calculate percentage differences
  CASE
    WHEN prod_raw_count > 0
    THEN ROUND((dev_raw_count - prod_raw_count) * 100.0 / prod_raw_count, 2)
    ELSE NULL
  END as raw_pct_diff,
  CASE
    WHEN prod_unique_orders > 0
    THEN ROUND((dev_unique_orders - prod_unique_orders) * 100.0 / prod_unique_orders, 2)
    ELSE NULL
  END as unique_pct_diff,
  -- Flag problematic hours
  CASE
    WHEN prod_max_submissions > 2 THEN 'HIGH_DUPLICATION'
    WHEN ABS(unique_diff) > 100 THEN 'LARGE_DISCREPANCY'
    WHEN prod_raw_count = 0 AND dev_raw_count > 0 THEN 'PROD_MISSING'
    WHEN dev_raw_count = 0 AND prod_raw_count > 0 THEN 'DEV_MISSING'
    ELSE 'NORMAL'
  END as status_flag
FROM hourly_comparison
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY
  ABS(unique_diff) DESC,
  order_date DESC,
  order_hour DESC
LIMIT 100;"""

def test_parsing():
    """Test parsing the complex query"""
    print("=" * 80)
    print("1. PARSING THE QUERY")
    print("=" * 80)

    # Parse the query
    ast = j.parse(QUERY)
    print(f"✅ Successfully parsed query")
    print(f"   Root node type: {type(ast.node).__name__}")
    print()

    return ast

def test_cte_extraction(ast):
    """Test finding all CTEs"""
    print("=" * 80)
    print("2. EXTRACTING CTEs")
    print("=" * 80)

    # Find all CTEs using Iterator
    ctes = Iterator(ast.node, exp.CTE).toList()
    print(f"✅ Found {len(ctes)} CTEs:")

    for i, cte in enumerate(ctes, 1):
        # Get the CTE name from the alias
        if hasattr(cte.node, 'alias') and cte.node.alias:
            # The alias might be a string or an object
            if isinstance(cte.node.alias, str):
                cte_name = cte.node.alias
            elif hasattr(cte.node.alias, 'this'):
                cte_name = cte.node.alias.this
            else:
                cte_name = str(cte.node.alias)
            print(f"   {i}. {cte_name}")
    print()

    return ctes

def test_table_references(ast):
    """Test extracting table references"""
    print("=" * 80)
    print("3. EXTRACTING TABLE REFERENCES")
    print("=" * 80)

    # Use the helper function
    tables = extract_table_references(ast.node)
    print(f"✅ Found {len(tables)} table references:")
    for table in tables:
        print(f"   - {table}")
    print()

    # Also find them using Iterator
    table_nodes = Iterator(ast.node, exp.Table).toList()
    print(f"✅ Iterator found {len(table_nodes)} table nodes")
    print()

    return tables

def test_case_expressions(ast):
    """Test finding CASE expressions"""
    print("=" * 80)
    print("4. FINDING CASE EXPRESSIONS")
    print("=" * 80)

    # Find all CASE expressions
    cases = Iterator(ast.node, exp.Case).toList()
    print(f"✅ Found {len(cases)} CASE expressions:")

    for i, case in enumerate(cases, 1):
        # Count WHEN clauses
        when_count = len(case.node.args.get('ifs', []))
        has_else = case.node.args.get('default') is not None
        print(f"   {i}. CASE with {when_count} WHEN clause(s), ELSE: {has_else}")

        # Show first condition if available
        if when_count > 0:
            first_when = case.node.args['ifs'][0]
            condition_sql = first_when.this.sql(dialect='bigquery')[:50]
            print(f"      First condition: {condition_sql}...")
    print()

    return cases

def test_aggregations(ast):
    """Test finding aggregation functions"""
    print("=" * 80)
    print("5. FINDING AGGREGATION FUNCTIONS")
    print("=" * 80)

    # Find all function calls
    all_funcs = Iterator(ast.node, exp.Func).toList()

    # Count by function name
    func_counts = {}
    for func in all_funcs:
        # Get the function name
        if hasattr(func.node, 'this'):
            func_name = str(func.node.this).upper()
            func_counts[func_name] = func_counts.get(func_name, 0) + 1

    # Display common aggregation functions
    agg_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ROUND', 'COALESCE', 'DATE', 'EXTRACT', 'TIMESTAMP_DIFF', 'DATE_SUB', 'CURRENT_DATE', 'ABS', 'TIMESTAMP']

    print(f"✅ Total function calls: {len(all_funcs)}")
    print("   Common functions found:")
    for func_name in sorted(func_counts.keys()):
        if func_name in agg_functions:
            print(f"   - {func_name}: {func_counts[func_name]} occurrence(s)")
    print()

def test_joins(ast):
    """Test finding JOIN operations"""
    print("=" * 80)
    print("6. FINDING JOIN OPERATIONS")
    print("=" * 80)

    # Find all JOINs
    joins = Iterator(ast.node, exp.Join).toList()
    print(f"✅ Found {len(joins)} JOIN operations:")

    for i, join in enumerate(joins, 1):
        join_kind = join.node.args.get('kind', 'INNER')
        # Get the table being joined
        if hasattr(join.node, 'this'):
            table_name = join.node.this.sql(dialect='bigquery')[:50]
            print(f"   {i}. {join_kind} JOIN on {table_name}...")
    print()

def test_where_conditions(ast):
    """Test finding WHERE conditions"""
    print("=" * 80)
    print("7. ANALYZING WHERE CONDITIONS")
    print("=" * 80)

    # Find all WHERE clauses
    wheres = Iterator(ast.node, exp.Where).toList()
    print(f"✅ Found {len(wheres)} WHERE clauses")

    # Find comparison operators
    comparisons = Iterator(ast.node, exp.Binary).filter(
        lambda n: isinstance(n.node, (exp.EQ, exp.GTE, exp.LT, exp.GT, exp.NEQ))
    ).toList()
    print(f"✅ Found {len(comparisons)} comparison operations")

    # Find AND/OR operations
    ands = Iterator(ast.node, exp.And).toList()
    ors = Iterator(ast.node, exp.Or).toList()
    print(f"✅ Found {len(ands)} AND operations")
    print(f"✅ Found {len(ors)} OR operations")
    print()

def test_transformations(ast):
    """Test query transformations"""
    print("=" * 80)
    print("8. TESTING TRANSFORMATIONS")
    print("=" * 80)

    # Example: Replace all literal 14 (Macy's retailer_id) with a parameter
    def replace_retailer_id(node):
        if isinstance(node, exp.Literal) and node.this == "14":
            return exp.Placeholder(this="retailer_id")
        return node

    # Create a copy and transform
    transformed = ast.transform(replace_retailer_id)

    # Count placeholders in transformed query
    placeholders = Iterator(transformed.node, exp.Placeholder).toList()
    print(f"✅ Replaced {len(placeholders)} literal values with parameters")

    # Show a snippet of the transformed SQL
    transformed_sql = transformed.sql(dialect='bigquery')
    # Find first occurrence of placeholder
    if '?' in transformed_sql or '@' in transformed_sql:
        print("✅ Transformation successful - parameters added")
    print()

def test_output_formatting(ast):
    """Test different output formats"""
    print("=" * 80)
    print("9. OUTPUT FORMATTING")
    print("=" * 80)

    # Get just the first CTE for brevity
    first_cte = Iterator(ast.node, exp.CTE).first()

    if first_cte:
        print("Original CTE (pretty formatted):")
        print("-" * 40)
        cte_sql = first_cte.sql(dialect='bigquery', pretty=True)
        # Show first 10 lines
        lines = cte_sql.split('\n')[:10]
        for line in lines:
            print(line)
        if len(cte_sql.split('\n')) > 10:
            print("...")
        print()

        print("Compact format:")
        print("-" * 40)
        compact = first_cte.sql(dialect='bigquery', pretty=False)[:200]
        print(compact + "...")
    print()

def main():
    print("🚀 Testing bsql.py with Complex BigQuery Query from Symbiosys")
    print()

    try:
        # Run all tests
        ast = test_parsing()
        ctes = test_cte_extraction(ast)
        tables = test_table_references(ast)
        cases = test_case_expressions(ast)
        test_aggregations(ast)
        test_joins(ast)
        test_where_conditions(ast)
        test_transformations(ast)
        test_output_formatting(ast)

        print("=" * 80)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()