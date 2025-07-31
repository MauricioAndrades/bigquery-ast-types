#!/usr/bin/env python3
"""
BigQuery AST Transformation CLI Tool

Transform simple SQL into production-ready BigQuery MERGE statements.

Usage:
    bq_transform.py [options] <input_sql>
    bq_transform.py --file <sql_file> [options]
    bq_transform.py --demo

Options:
    -h, --help              Show this help message
    -f, --file FILE         Read SQL from file
    -r, --retailer ID       Retailer ID (required)
    -p, --project PROJECT   BigQuery project ID
    -d, --dataset DATASET   BigQuery dataset name
    -t, --threshold DATE    DateTime threshold (default: 30 days ago)
    -o, --output FILE       Write output to file
    --analyze               Only analyze the query, don't transform
    --validate              Validate the generated SQL
    --demo                  Run demo with example SQL

Examples:
    # Transform inline SQL
    ./bq_transform.py -r 12345 "SELECT * FROM orders WHERE date > '2024-01-01'"

    # Transform from file
    ./bq_transform.py -f my_query.sql -r 12345 -p my-project -d analytics

    # Analyze only
    ./bq_transform.py --analyze -f complex_query.sql

    # Run demo
    ./bq_transform.py --demo

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from alib import (
    OrderMergeBuilder,
    QueryAnalyzer,
    RetailerPatterns,
    DuplicateAnalysisBuilder,
    ComprehensiveMergeBuilder,
)
from jsql import j
import sqlglot


class Colors:
    """Terminal colors for pretty output."""

    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def print_header(text: str):
    """Print a colored header."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}")
    print("=" * len(text))


def print_success(text: str):
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")


def print_error(text: str):
    """Print error message."""
    print(f"{Colors.FAIL}❌ {text}{Colors.ENDC}")


def print_info(text: str):
    """Print info message."""
    print(f"{Colors.CYAN}ℹ {text}{Colors.ENDC}")


def analyze_query(sql: str):
    """Analyze and display query information."""
    print_header("Query Analysis")

    try:
        analysis = QueryAnalyzer.analyze_query(sql)

        print(f"Tables: {Colors.BOLD}{', '.join(analysis['tables'])}{Colors.ENDC}")
        print(f"Columns: {analysis['columns']} unique")
        print(f"Aggregations: {analysis['aggregations']}")
        print(f"Joins: {analysis['join_count']}")
        print(f"Subqueries: {analysis['subquery_count']}")
        print(f"Complexity Score: {analysis['complexity_score']}")

        if analysis["has_cte"]:
            print_info("Query uses CTEs")

        return True
    except Exception as e:
        print_error(f"Analysis failed: {e}")
        return False


def transform_query(
    sql: str, retailer_id: int, project: str, dataset: str, datetime_threshold: str
) -> str:
    """Transform SQL to order merge pattern."""
    print_header("Transformation")

    try:
        builder = OrderMergeBuilder(project, dataset)
        result = builder.build_3cte_merge(
            source_sql=sql,
            retailer_id=retailer_id,
            datetime_threshold=datetime_threshold,
        )

        print_success("Transformation complete")
        print_info(f"Target table: `{project}.{dataset}.order`")
        print_info(f"Retailer ID: {retailer_id}")
        print_info(f"Threshold: {datetime_threshold}")

        return result
    except Exception as e:
        print_error(f"Transformation failed: {e}")
        raise


def validate_sql(sql: str) -> bool:
    """Validate generated SQL."""
    print_header("Validation")

    try:
        parsed = sqlglot.parse_one(sql, read="bigquery")
        print_success("SQL syntax is valid")

        # Check for required components
        ctes = list(parsed.find_all(sqlglot.exp.CTE))
        if ctes:
            print_success(f"Found {len(ctes)} CTEs")

        merge = parsed.find(sqlglot.exp.Merge)
        if merge:
            print_success("Contains MERGE statement")

        return True
    except Exception as e:
        print_error(f"Validation failed: {e}")
        return False


def run_demo():
    """Run demo transformation."""
    print_header("BQAST Demo - Order Merge Transformation")

    demo_sql = """
    SELECT 
        o.order_id,
        o.product_id,
        c.customer_id as visitor_id,
        o.order_date as order_ts,
        o.quantity,
        p.price
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date >= '2024-01-01'
    """

    print("\nInput SQL:")
    print(demo_sql)

    # Analyze
    analyze_query(demo_sql)

    # Transform
    result = transform_query(
        demo_sql,
        retailer_id=99999,
        project="demo-project",
        dataset="analytics",
        datetime_threshold="2024-01-01 00:00:00",
    )

    # Show snippet of result
    print("\nGenerated MERGE (first 500 chars):")
    print(result[:500] + "...")

    # Validate
    validate_sql(result)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Transform SQL to BigQuery order merge pattern",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s -r 12345 "SELECT * FROM orders"
  %(prog)s -f query.sql -r 12345 -p my-project
  %(prog)s --demo
        """,
    )

    parser.add_argument("sql", nargs="?", help="SQL query to transform")
    parser.add_argument("-f", "--file", help="Read SQL from file")
    parser.add_argument("-r", "--retailer", type=int, help="Retailer ID")
    parser.add_argument(
        "-p", "--project", default="my-project", help="BigQuery project"
    )
    parser.add_argument(
        "-d", "--dataset", default="event_analytics", help="BigQuery dataset"
    )
    parser.add_argument(
        "-t", "--threshold", help="DateTime threshold (YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument("-o", "--output", help="Output file")
    parser.add_argument("--analyze", action="store_true", help="Only analyze query")
    parser.add_argument(
        "--validate", action="store_true", help="Validate generated SQL"
    )
    parser.add_argument("--demo", action="store_true", help="Run demo")
    parser.add_argument(
        "--duplicate-analysis",
        action="store_true",
        help="Generate duplicate analysis query",
    )
    parser.add_argument(
        "--start-date", help="Start date for duplicate analysis (YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument(
        "--end-date", help="End date for duplicate analysis (YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument(
        "--comprehensive-merge",
        action="store_true",
        help="Generate comprehensive MERGE DELETE query"
    )
    parser.add_argument(
        "--filter-start", help="Filter start date for comprehensive merge"
    )
    parser.add_argument(
        "--filter-end", help="Filter end date for comprehensive merge"
    )

    args = parser.parse_args()

    # Handle demo mode
    if args.demo:
        run_demo()
        return 0

    # Handle duplicate analysis mode
    if args.duplicate_analysis:
        if not args.start_date or not args.end_date:
            print_error(
                "Both --start-date and --end-date are required for duplicate analysis"
            )
            return 1

        print_header("Duplicate Analysis Query Generation")

        builder = DuplicateAnalysisBuilder(args.project, args.dataset)
        result = builder.build_duplicate_analysis(args.start_date, args.end_date)

        print_success("Query generated")
        print_info(f"Project: {args.project}")
        print_info(f"Dataset: {args.dataset}")
        print_info(f"Date range: {args.start_date} to {args.end_date}")

        # Output
        if args.output:
            with open(args.output, "w") as f:
                f.write(result)
            print_success(f"Output written to {args.output}")
        else:
            print("\nGenerated SQL:")
            print(result)

        return 0
    
    # Handle comprehensive merge mode
    if args.comprehensive_merge:
        if not all([args.start_date, args.end_date, args.filter_start, args.filter_end]):
            print_error(
                "All dates required: --start-date, --end-date, --filter-start, --filter-end"
            )
            return 1
        
        print_header("Comprehensive MERGE DELETE Query Generation")
        
        builder = ComprehensiveMergeBuilder(args.project, args.dataset)
        result = builder.build_comprehensive_merge(
            args.start_date, args.end_date, args.filter_start, args.filter_end
        )
        
        print_success("Query generated")
        print_info(f"Project: {args.project}")
        print_info(f"Dataset: {args.dataset}")
        print_info(f"Source date range: {args.start_date} to {args.end_date}")
        print_info(f"Filter date range: {args.filter_start} to {args.filter_end}")
        
        # Output
        if args.output:
            with open(args.output, "w") as f:
                f.write(result)
            print_success(f"Output written to {args.output}")
        else:
            print("\nGenerated SQL:")
            print(result)
        
        return 0

    # Get SQL input
    if args.file:
        try:
            with open(args.file, "r") as f:
                sql = f.read()
        except Exception as e:
            print_error(f"Failed to read file: {e}")
            return 1
    elif args.sql:
        sql = args.sql
    else:
        parser.print_help()
        return 1

    # Analyze only mode
    if args.analyze:
        analyze_query(sql)
        return 0

    # Full transformation
    if not args.retailer:
        print_error("Retailer ID is required for transformation")
        return 1

    # Default threshold to 30 days ago
    if not args.threshold:
        threshold = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        threshold = args.threshold

    # Analyze first
    analyze_query(sql)

    # Transform
    try:
        result = transform_query(
            sql,
            retailer_id=args.retailer,
            project=args.project,
            dataset=args.dataset,
            datetime_threshold=threshold,
        )

        # Validate if requested
        if args.validate:
            validate_sql(result)

        # Output
        if args.output:
            with open(args.output, "w") as f:
                f.write(result)
            print_success(f"Output written to {args.output}")
        else:
            print("\nGenerated SQL:")
            print(result)

        return 0

    except Exception as e:
        print_error(f"Transformation failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
