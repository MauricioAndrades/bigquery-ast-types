"""
AST Library - Reusable Query Patterns

Programmatic query builders for common BigQuery patterns.
These are the building blocks for order merge transformations.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import List, Dict, Optional, Tuple
import sqlglot
from sqlglot import exp
from jsql import (
    j, JSQLNode, null_safe_eq, standardize_string_id, 
    standardize_numeric, standardize_boolean, deep_copy_transform,
    extract_table_references, inject_cte
)


class OrderMergeBuilder:
    """Builder for order merge query patterns."""
    
    def __init__(self, project: str, dataset: str = "event_analytics"):
        self.project = project
        self.dataset = dataset
        self.target_table = f"`{project}.{dataset}.order`"
        
        # Column type mapping for standardization
        self.column_types = {
            'visitor_id': 'string_id',
            'order_id': 'string_id',
            'product_id': 'string_id',
            'session_id': 'string_id',
            'crm_id': 'string_id',
            'quantity': 'integer',
            'platform': 'integer',
            'regular_unit_price': 'numeric',
            'discount_unit_price': 'numeric',
            'is_restaurant': 'boolean',
            'is_recurring': 'boolean',
            'user_tracking_allowed': 'boolean'
        }
    
    def build_3cte_merge(self, 
                         source_sql: str, 
                         retailer_id: int,
                         datetime_threshold: str) -> str:
        """Build complete 3-CTE merge pattern from source SQL."""
        
        # Parse source query
        source = j.parse(source_sql)
        
        # Build CTEs
        raw_cte = self._build_raw_orders_cte(source)
        cleaned_cte = self._build_cleaned_orders_cte(retailer_id)
        deduped_cte = self._build_deduped_orders_cte()
        
        # Build MERGE statement
        merge = self._build_merge_statement(retailer_id, datetime_threshold)
        
        # Combine into final query
        final_query = f"""
WITH
{raw_cte.sql()},
{cleaned_cte.sql()},
{deduped_cte.sql()}
{merge.sql(pretty=True)}
"""
        
        return final_query
    
    def _build_raw_orders_cte(self, source_query: JSQLNode) -> JSQLNode:
        """Build raw_orders CTE from source query."""
        return j.CTE("raw_orders", source_query.node)
    
    def _build_cleaned_orders_cte(self, retailer_id: int) -> JSQLNode:
        """Build cleaned_orders CTE with standardization using native sqlglot."""
        
        # Parse a template to get column structure
        template = sqlglot.parse_one("SELECT * FROM raw_orders", read="bigquery")
        
        # Build new select with transformations
        select = exp.Select()
        select = select.from_("raw_orders")
        
        # Add standard columns
        select = select.select(
            exp.alias_(exp.Literal.number(retailer_id), "retailer_id"),
            exp.alias_(exp.func("CURRENT_TIMESTAMP"), "symbiosys_ts")
        )
        
        # Use deep transformation for standardization
        def standardize_columns(node):
            if isinstance(node, exp.Column) and not node.table:
                col_name = node.name
                col_type = self.column_types.get(col_name)
                
                if col_type == 'string_id':
                    return standardize_string_id(node)
                elif col_type == 'integer':
                    return exp.SafeCast(this=node, to=exp.DataType.build("INT64"))
                elif col_type == 'numeric':
                    return standardize_numeric(node)
                elif col_type == 'boolean':
                    return standardize_boolean(node)
            
            return node
        
        # Get all columns from raw_orders and standardize them
        raw_select = sqlglot.parse_one("SELECT * FROM raw_orders", read="bigquery")
        
        # Assuming we know the columns, add them with standardization
        for col_name in self.column_types:
            col_expr = exp.Column(this=col_name)
            standardized = standardize_columns(col_expr)
            select = select.select(exp.alias_(standardized, col_name))
        
        # Add defaults
        select = self._add_default_columns(select)
        
        return j.CTE("cleaned_orders", select)
    
    def _build_deduped_orders_cte(self) -> JSQLNode:
        """Build deduped_orders CTE with ROW_NUMBER deduplication."""
        
        dedup_sql = """
SELECT * EXCEPT(rank)
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY retailer_id, order_id, product_id, visitor_id, order_ts, session_id
            ORDER BY order_ts DESC, symbiosys_ts DESC
        ) AS rank
    FROM cleaned_orders
    WHERE retailer_id IS NOT NULL
        AND order_id IS NOT NULL AND order_id != ''
        AND product_id IS NOT NULL AND product_id != ''
        AND visitor_id IS NOT NULL AND visitor_id != ''
        AND order_ts IS NOT NULL
)
WHERE rank = 1
"""
        
        return j.CTE("deduped_orders", sqlglot.parse_one(dedup_sql))
    
    def _build_merge_statement(self, retailer_id: int, datetime_threshold: str) -> JSQLNode:
        """Build MERGE statement."""
        
        # Key columns for merge
        merge_keys = ['retailer_id', 'order_id', 'product_id', 'visitor_id', 'order_ts']
        
        # Build ON conditions with null-safe equality
        on_conditions = []
        for key in merge_keys:
            left = exp.Column(this=key, table="S")
            right = exp.Column(this=key, table="T")
            on_conditions.append(null_safe_eq(left, right))
        
        # Add timestamp condition
        on_conditions.append(
            exp.GTE(
                this=exp.Column(this="order_ts", table="T"),
                expression=exp.func("TIMESTAMP", exp.Literal.string(datetime_threshold))
            )
        )
        
        # Combine conditions
        on_condition = on_conditions[0]
        for cond in on_conditions[1:]:
            on_condition = exp.And(this=on_condition, expression=cond)
        
        # Build MERGE
        merge_sql = f"""
MERGE INTO {self.target_table} T
USING (SELECT * FROM deduped_orders) S
ON {on_condition.sql()}
WHEN NOT MATCHED BY TARGET THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE 
    AND T.retailer_id = {retailer_id}
    AND T.order_ts >= TIMESTAMP("{datetime_threshold}")
THEN DELETE
"""
        
        return j.parse(merge_sql)
    
    def _add_default_columns(self, select: exp.Select) -> exp.Select:
        """Add any missing columns with default values."""
        
        default_values = {
            'session_id': exp.Literal.string(""),
            'crm_id': exp.Null(),
            'platform': exp.Literal.number(1),
            'user_ip': exp.Null(),
            'zip_code': exp.Null(),
            'hashed_email': exp.Null(),
            'hashed_pii': exp.Array(expressions=[]),
            'merchant_id': exp.Null(),
            'child_product_id': exp.Null(),
            'country': exp.Null(),
            'seller_id': exp.Null()
        }
        
        # Get existing column names
        existing_cols = set()
        for expr in select.expressions:
            if isinstance(expr, exp.Alias):
                existing_cols.add(expr.alias)
            elif isinstance(expr, exp.Column):
                existing_cols.add(expr.name)
        
        # Add missing columns
        for col_name, default_expr in default_values.items():
            if col_name not in existing_cols:
                select = select.select(exp.alias_(default_expr, col_name))
        
        return select


class DedupPatterns:
    """Common deduplication patterns."""
    
    @staticmethod
    def simple_dedup(partition_by: List[str], order_by: List[Tuple[str, str]] = None) -> str:
        """Simple ROW_NUMBER deduplication."""
        
        if order_by is None:
            order_by = [("symbiosys_ts", "DESC")]
        
        partition_clause = ", ".join(partition_by)
        order_clause = ", ".join(f"{col} {dir}" for col, dir in order_by)
        
        return f"""
SELECT * EXCEPT(rn)
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {partition_clause}
            ORDER BY {order_clause}
        ) AS rn
    FROM source_table
)
WHERE rn = 1
"""
    
    @staticmethod
    def comprehensive_dedup(all_columns: List[str]) -> str:
        """Comprehensive deduplication partitioning by ALL columns."""
        
        # Special handling for certain columns
        partition_exprs = []
        
        for col in all_columns:
            if col == 'child_product_id':
                # COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))
                partition_exprs.append(
                    "COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))"
                )
            elif col == 'hashed_pii':
                # Array serialization
                partition_exprs.append("""
ARRAY_TO_STRING(
    ARRAY(
        SELECT CONCAT(pii.key, ':', pii.value)
        FROM UNNEST(hashed_pii) AS pii
        WHERE pii.key IS NOT NULL AND pii.value IS NOT NULL
        ORDER BY pii.key, pii.value
    ),
    ', '
)""".strip())
            else:
                partition_exprs.append(col)
        
        partition_clause = ",\n        ".join(partition_exprs)
        
        return f"""
SELECT * EXCEPT(rn)
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY
                {partition_clause}
            ORDER BY order_ts ASC, symbiosys_ts ASC
        ) AS rn
    FROM source_table
)
WHERE rn = 1
"""


class StandardizationPatterns:
    """Common standardization patterns using sqlglot transforms."""
    
    @staticmethod
    def standardize_columns(columns: Dict[str, str]) -> List[exp.Expression]:
        """Generate standardized column expressions based on type."""
        
        standardized = []
        
        for col_name, col_type in columns.items():
            col_ref = exp.Column(this=col_name)
            
            if col_type == 'string_id':
                expr = standardize_string_id(col_ref)
            elif col_type == 'integer':
                expr = exp.SafeCast(this=col_ref, to=exp.DataType.build("INT64"))
            elif col_type == 'numeric':
                expr = standardize_numeric(col_ref)
            elif col_type == 'boolean':
                expr = standardize_boolean(col_ref)
            else:
                expr = col_ref
            
            standardized.append(exp.alias_(expr, col_name))
        
        return standardized
    
    @staticmethod
    def apply_to_query(query: exp.Expression, columns: Dict[str, str]) -> exp.Expression:
        """Apply standardization to all matching columns in a query."""
        
        def standardize_node(node):
            if isinstance(node, exp.Column) and node.name in columns:
                col_type = columns[node.name]
                
                # Don't transform if already in a function
                parent = node.parent
                if isinstance(parent, exp.Func):
                    return node
                
                if col_type == 'string_id':
                    return standardize_string_id(node)
                elif col_type == 'integer':
                    return exp.SafeCast(this=node, to=exp.DataType.build("INT64"))
                elif col_type == 'numeric':
                    return standardize_numeric(node)
                elif col_type == 'boolean':
                    return standardize_boolean(node)
            
            return node
        
        return query.transform(standardize_node)


class QueryAnalyzer:
    """Analyze queries for optimization opportunities."""
    
    @staticmethod
    def analyze_query(sql: str) -> Dict[str, any]:
        """Analyze a query and return insights."""
        ast = sqlglot.parse_one(sql, read="bigquery")
        
        # Extract all tables
        tables = extract_table_references(ast)
        
        # Find all columns
        columns = []
        for col in ast.find_all(exp.Column):
            full_name = f"{col.table}.{col.name}" if col.table else col.name
            columns.append(full_name)
        
        # Find aggregations
        aggregations = []
        for func in ast.find_all(exp.AggFunc):
            aggregations.append(func.sql())
        
        # Check for JOINs
        joins = list(ast.find_all(exp.Join))
        
        # Check for subqueries
        subqueries = []
        for node in ast.walk():
            if isinstance(node, exp.Subquery):
                subqueries.append(node)
        
        return {
            "tables": tables,
            "columns": columns,
            "aggregations": aggregations,
            "join_count": len(joins),
            "subquery_count": len(subqueries),
            "has_cte": bool(ast.args.get("with")),
            "complexity_score": len(tables) + len(joins) + len(subqueries)
        }


class RetailerPatterns:
    """Retailer-specific query patterns."""
    
    @staticmethod
    def chewy_full_outer_join(datetime_threshold: str) -> str:
        """Chewy's FULL OUTER JOIN pattern."""
        
        return f"""
SELECT
    COALESCE(RO.order_time_placed, NMO.order_time_placed) as order_time_placed,
    COALESCE(RO.visitor_id, NMO.visitor_id) as visitor_id,
    COALESCE(RO.session_id, NMO.session_id) as session_id,
    COALESCE(RO.order_id, NMO.order_id) as order_id,
    COALESCE(RO.product_sku, NMO.product_sku) as product_sku,
    COALESCE(RO.quantity, NMO.quantity) as quantity,
    COALESCE(RO.purchase_price, NMO.purchase_price) as purchase_price,
    COALESCE(RO.csr_flag, NMO.csr_flag) as csr_flag,
    COALESCE(RO.autoship_flag, NMO.autoship_flag) as autoship_flag
FROM (
    SELECT * FROM `symbiosys-prod.chewy.raw_orders_v3`
    WHERE order_time_placed >= DATETIME(TIMESTAMP('{datetime_threshold}'))
) RO
FULL OUTER JOIN (
    SELECT * FROM `symbiosys-prod.chewy.raw_nma_orders_v3`
    WHERE order_time_placed >= DATETIME(TIMESTAMP('{datetime_threshold}'))
) NMO
ON RO.order_id = NMO.order_id AND RO.product_sku = NMO.product_sku
"""
    
    @staticmethod
    def visitor_blacklist_filter() -> exp.Expression:
        """Visitor blacklist filter pattern."""
        
        return exp.And(
            this=exp.Is(
                this=exp.Column(this="visitor_id", table="vib"),
                expression=exp.Null()
            ),
            expression=exp.NEQ(
                this=exp.Column(this="csr_flag"),
                expression=exp.TRUE
            )
        )