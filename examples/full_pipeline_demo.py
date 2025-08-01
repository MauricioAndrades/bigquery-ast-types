import importlib.util
import sys

module_name = "sqlglot_manual"
file_path = "/Users/op/Documents/sym/symbo/apps/composer/venv/lib/python3.11/site-packages/sqlglot/__init__.py"

spec = importlib.util.spec_from_file_location(module_name, file_path)
sqlglot_manual = importlib.util.module_from_spec(spec)  # type: ignore
sys.modules[module_name] = sqlglot_manual
spec.loader.exec_module(sqlglot_manual)  # type: ignore

# Now you can use sqlglot_manual.<something>

# Now you can use sql.<something>

from alib import OrderMergeBuilder, QueryAnalyzer, RetailerPatterns
from lib.bsql import j, extract_table_references
import sqlglot
from sqlglot import expressions as exp


def analyze_input_query(sql: str):
    """
    Analyze the input query to understand its structure.
    Steps:
    1. Uses QueryAnalyzer to parse the SQL and extract metadata.
    2. Returns a dictionary with tables, columns, aggregations, joins, subqueries, and complexity score.
    """
    analysis = QueryAnalyzer.analyze_query(sql)
    return analysis


def transform_columns_example(sql: str):
    """
    Show column transformation in action.
    Steps:
    1. Parse the SQL into an AST using j.parse.
    2. Identify columns that are likely to be IDs.
    3. For each such column, wrap it in NULLIF(TRIM(...), "") to treat empty/whitespace as NULL.
    4. Return the transformed SQL as a string.
    """
    query = j.parse(sql)
    id_columns = ["order_id", "product_id", "visitor_id", "customer_id", "session_id"]
    query.find(exp.Column, predicate=lambda n: n.name in id_columns).replaceWith(
        lambda n: exp.func("NULLIF", exp.func("TRIM", n.node), exp.Literal.string(""))
    )
    return query.sql(pretty=True)


def full_pipeline_example():
    """
    Complete pipeline from simple SQL to production MERGE.
    Steps:
    1. Define a developer-written SQL query.
    2. Analyze the query structure and metadata.
    3. Apply column transformations to standardize ID columns.
    4. Generate a production-ready MERGE statement using OrderMergeBuilder.
    5. Validate the generated SQL for syntax and required components.
    6. Return a dictionary with all intermediate and final results.
    """
    developer_sql = """
    SELECT
        o.order_id,
        o.product_id,
        c.customer_id as visitor_id,
        o.order_date as order_ts,
        o.quantity,
        p.price,
        c.email,
        c.loyalty_status
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date >= '2024-01-01'
        AND o.status = 'COMPLETED'
    """
    analysis = analyze_input_query(developer_sql)
    transformed_sql = transform_columns_example(developer_sql)
    builder = OrderMergeBuilder("my-project", "analytics")
    production_merge = builder.build_3cte_merge(
        source_sql=developer_sql,
        retailer_id=99999,
        datetime_threshold="2024-01-01 00:00:00",
    )
    validation = {}
    try:
        parsed = sqlglot.parse_one(production_merge, read="bigquery")
        ctes = list(parsed.find_all(exp.CTE))
        cte_names = [cte.alias for cte in ctes]
        merge = parsed.find(exp.Merge)
        final_tables = extract_table_references(parsed)
        validation = {
            "valid_syntax": True,
            "cte_count": len(ctes),
            "cte_names": cte_names,
            "has_merge": merge is not None,
            "final_tables": final_tables,
        }
    except Exception as e:
        validation = {"valid_syntax": False, "error": str(e)}
    return {
        "developer_sql": developer_sql,
        "analysis": analysis,
        "transformed_sql": transformed_sql,
        "production_merge": production_merge,
        "validation": validation,
    }


def retailer_specific_example():
    """
    Show retailer-specific transformations.
    Steps:
    1. Define a dictionary of retailers with their IDs and special logic.
    2. For each retailer, describe their unique SQL transformation needs.
    3. For Chewy, generate a sample SQL pattern using RetailerPatterns.
    4. Return the retailer configuration and example SQLs.
    """
    retailers = {
        "Chewy": {
            "id": 1001,
            "special_logic": "FULL OUTER JOIN between raw_orders and raw_nma_orders",
            "unique_columns": ["csr_flag", "autoship_flag"],
        },
        "Best Buy": {
            "id": 2001,
            "special_logic": "Product hierarchy flattening",
            "unique_columns": ["store_id", "online_exclusive"],
        },
        "Target": {
            "id": 3001,
            "special_logic": "RedCard discount handling",
            "unique_columns": ["redcard_discount", "circle_rewards"],
        },
    }
    chewy_sql = RetailerPatterns.chewy_full_outer_join("2024-01-01 00:00:00")
    return {"retailers": retailers, "chewy_sql_pattern": chewy_sql[:200] + "..."}


def performance_considerations():
    """
    Discuss performance optimizations.
    Steps:
    1. List common SQL performance techniques for BigQuery.
    2. For each technique, provide a description and example.
    3. Return the list of optimizations.
    """
    optimizations = [
        {
            "technique": "Partition Pruning",
            "description": "Always include partition filters in WHERE clauses",
            "example": "WHERE order_ts >= TIMESTAMP('2024-01-01')",
        },
        {
            "technique": "Clustering",
            "description": "Order MERGE keys to match table clustering",
            "example": "PARTITION BY retailer_id, order_date, order_id",
        },
        {
            "technique": "Incremental Processing",
            "description": "Use datetime thresholds to limit data scanned",
            "example": "T.order_ts >= TIMESTAMP(@start_date)",
        },
        {
            "technique": "Deduplication Strategy",
            "description": "ROW_NUMBER() with targeted partitioning",
            "example": "ROW_NUMBER() OVER (PARTITION BY key_columns ORDER BY ts DESC)",
        },
    ]
    return optimizations


if __name__ == "__main__":
    # Run the full pipeline and get all results as a dictionary
    pipeline_results = full_pipeline_example()
    retailer_info = retailer_specific_example()
    perf_opts = performance_considerations()
    # Results are now available in pipeline_results, retailer_info, and perf_opts for further use or testing
