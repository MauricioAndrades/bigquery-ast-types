"""
AST Library - Reusable Query Patterns

Programmatic query builders for common BigQuery patterns.
These are the building blocks for order merge transformations.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os


from typing import (
    Any,
    Callable,
    Optional,
    List,
    Union,
    Iterator as PyIterator,
    TypeVar,
    Generic,
    Tuple,
    Dict,
)
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "sqlglot"))

import sqlglot
from sqlglot import exp

from jsql import (
    j,
    JSQLNode,
    null_safe_eq,
    standardize_string_id,
    standardize_numeric,
    standardize_boolean,
    deep_copy_transform,
    extract_table_references,
    inject_cte,
)


class ComprehensiveMergeBuilder:
    """Build comprehensive MERGE DELETE queries with full column comparison."""
    
    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
        self.target_table = f"`{dataset}.order`"
        self.staging_table = f"`{project}.{dataset}_staging.order`"
        
        # All columns in order table
        self.all_columns = [
            "retailer_id",
            "order_id", 
            "order_ts",
            "session_id",
            "visitor_id",
            "crm_id",
            "platform",
            "user_ip",
            "zip_code",
            "hashed_email",
            "hashed_pii",  # Array type
            "product_id",
            "quantity",
            "regular_unit_price",
            "discount_unit_price",
            "merchant_id",
            "country",
            "is_restaurant",
            "is_recurring",
            "user_tracking_allowed",
            "seller_id",
            "child_product_id",
            "symbiosys_ts"
        ]
    
    def build_comprehensive_merge(self,
                                start_date: str,
                                end_date: str,
                                filter_start: str,
                                filter_end: str) -> str:
        """Build the exact comprehensive MERGE DELETE pattern."""
        
        # Build partition columns for dedup
        partition_columns = self._build_partition_columns()
        
        # Build ON clause conditions
        on_conditions = self._build_on_conditions(filter_start, filter_end)
        
        return f"""MERGE {self.target_table} AS T USING (
  WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        {partition_columns}
      ORDER BY
        order_ts ASC,
        symbiosys_ts ASC
    ) AS rn
  FROM {self.staging_table}
  WHERE order_ts >= TIMESTAMP('{start_date}')
    AND order_ts <  TIMESTAMP('{end_date}')
)
SELECT * FROM deduped
)  AS B
ON
{on_conditions}

WHEN MATCHED THEN
  DELETE;"""
    
    def _build_partition_columns(self) -> str:
        """Build PARTITION BY clause with special handling."""
        
        partition_exprs = []
        
        for col in self.all_columns:
            if col == 'child_product_id':
                # Special normalization
                partition_exprs.append(
                    "COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))"
                )
            elif col == 'hashed_pii':
                # Array serialization
                partition_exprs.append("""ARRAY_TO_STRING(
          ARRAY(
            SELECT CONCAT(pii.key, ':', pii.value)
            FROM UNNEST(hashed_pii) AS pii
            WHERE pii.key   IS NOT NULL
              AND pii.value IS NOT NULL
            ORDER BY pii.key, pii.value
          ),
          ', '
        )""")
            else:
                partition_exprs.append(col)
        
        return ",\n        ".join(partition_exprs)
    
    def _build_on_conditions(self, filter_start: str, filter_end: str) -> str:
        """Build ON clause with null-safe comparisons for all columns."""
        
        conditions = []
        
        # retailer_id (special - not null-safe)
        conditions.append("  -- retailer and order identifiers")
        conditions.append("  T.retailer_id = B.retailer_id")
        
        # order_id (null-safe)
        conditions.append("  AND (T.order_id = B.order_id OR (T.order_id IS NULL AND B.order_id IS NULL))")
        
        # timestamps
        conditions.append("\n  -- timestamps")
        conditions.append("  AND (T.order_ts = B.order_ts OR (T.order_ts IS NULL AND B.order_ts IS NULL))")
        
        # scalar fields
        conditions.append("\n  -- all the rest of the scalar fields, null‑safe")
        scalar_fields = [
            "session_id", "visitor_id", "crm_id", "platform", 
            "user_ip", "zip_code", "hashed_email"
        ]
        
        for field in scalar_fields:
            conditions.append(f"  AND (T.{field} = B.{field} OR (T.{field} IS NULL AND B.{field} IS NULL))")
        
        # hashed_pii array (special handling)
        conditions.append("""
  -- inline, sorted fingerprint of the PII array
  AND (
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(x.key, ':', x.value)
        FROM UNNEST(T.hashed_pii) AS x
        WHERE x.key   IS NOT NULL
          AND x.value IS NOT NULL
        ORDER BY x.key, x.value
      ),
      ', '
    )
    =
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(x.key, ':', x.value)
        FROM UNNEST(B.hashed_pii) AS x
        WHERE x.key   IS NOT NULL
          AND x.value IS NOT NULL
        ORDER BY x.key, x.value
      ),
      ', '
    )
  )""")
        
        # More scalar fields
        more_fields = [
            "product_id", "quantity", "regular_unit_price", 
            "discount_unit_price", "merchant_id", "country"
        ]
        
        conditions.append("")
        for field in more_fields:
            # Add extra spacing for alignment
            padding = " " * (20 - len(field))
            conditions.append(f"  AND (T.{field}{padding} = B.{field}{padding} OR (T.{field}{padding} IS NULL AND B.{field}{padding} IS NULL))")
        
        # Boolean flags
        conditions.append("\n  -- boolean flags with null-safe comparison")
        bool_fields = ["is_restaurant", "is_recurring", "user_tracking_allowed"]
        
        for field in bool_fields:
            conditions.append(f"  AND (T.{field} = B.{field} OR (T.{field} IS NULL AND B.{field} IS NULL))")
        
        # seller_id
        conditions.append("")
        conditions.append("  AND (T.seller_id            = B.seller_id            OR (T.seller_id            IS NULL AND B.seller_id            IS NULL))")
        
        # child_product_id with normalization
        conditions.append("""
  -- child_product_id with same normalization as backup
  AND (
    COALESCE(NULLIF(TRIM(T.child_product_id), ''), CAST(T.product_id AS STRING)) = 
    COALESCE(NULLIF(TRIM(B.child_product_id), ''), CAST(B.product_id AS STRING))
  )""")
        
        # symbiosys_ts
        conditions.append("""
  -- finally, symbiosys_ts
  AND (T.symbiosys_ts       = B.symbiosys_ts       OR (T.symbiosys_ts       IS NULL AND B.symbiosys_ts       IS NULL))""")
        
        # Date filter
        conditions.append(f"    AND T.order_ts >= TIMESTAMP('{filter_start}')")
        conditions.append(f"    AND T.order_ts <  TIMESTAMP('{filter_end}')")
        
        return "\n".join(conditions)


class DuplicateAnalysisBuilder:
    """Build duplicate analysis queries for staging data."""

    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
        self.staging_table = f"`{project}.{dataset}_staging.order`"

    def build_duplicate_analysis(self, start_date: str, end_date: str) -> str:
        """Build the exact duplicate analysis query pattern."""

        # All columns to partition by (comprehensive dedup)
        partition_columns = [
            "retailer_id",
            "order_id",
            "COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))",
            "session_id",
            "visitor_id",
            "crm_id",
            "platform",
            "user_ip",
            "zip_code",
            "hashed_email",
            """ARRAY_TO_STRING(
          ARRAY(
            SELECT CONCAT(pii.key, ':', pii.value)
            FROM UNNEST(hashed_pii) AS pii
            WHERE pii.key   IS NOT NULL
              AND pii.value IS NOT NULL
            ORDER BY pii.key, pii.value
          ),
          ', '
        )""",
            "product_id",
            "quantity",
            "regular_unit_price",
            "discount_unit_price",
            "merchant_id",
            "country",
            "is_restaurant",
            "is_recurring",
            "user_tracking_allowed",
            "seller_id",
        ]

        partition_clause = ",\n        ".join(partition_columns)

        return f"""-- Check for duplicates in staging
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        {partition_clause}
      ORDER BY
        order_ts ASC,
        symbiosys_ts ASC
    ) AS rn
  FROM {self.staging_table}
  WHERE order_ts >= TIMESTAMP('{start_date}')
    AND order_ts <  TIMESTAMP('{end_date}')
)
SELECT
  retailer_id,
  COUNT(*) as total_rows,
  COUNTIF(rn > 1) as duplicate_rows,
  ROUND(COUNTIF(rn > 1) * 100.0 / COUNT(*), 2) as duplicate_percentage
FROM deduped
GROUP BY retailer_id
ORDER BY duplicate_percentage DESC"""


class OrderMergeBuilder:
    """Builder for order merge query patterns."""

    def __init__(self, project: str, dataset: str = "event_analytics"):
        self.project = project
        self.dataset = dataset
        self.target_table = f"`{project}.{dataset}.order`"

        # Column type mapping for standardization
        self.column_types = {
            "visitor_id": "string_id",
            "order_id": "string_id",
            "product_id": "string_id",
            "session_id": "string_id",
            "crm_id": "string_id",
            "quantity": "integer",
            "platform": "integer",
            "regular_unit_price": "numeric",
            "discount_unit_price": "numeric",
            "is_restaurant": "boolean",
            "is_recurring": "boolean",
            "user_tracking_allowed": "boolean",
        }

    def build_3cte_merge(
        self, source_sql: str, retailer_id: int, datetime_threshold: str
    ) -> str:
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
            exp.alias_(exp.func("CURRENT_TIMESTAMP"), "symbiosys_ts"),
        )

        # Use deep transformation for standardization
        def standardize_columns(node):
            if isinstance(node, exp.Column) and not node.table:
                col_name = node.name
                col_type = self.column_types.get(col_name)

                if col_type == "string_id":
                    return standardize_string_id(node)
                elif col_type == "integer":
                    return exp.Cast(this=node, to=exp.DataType.build("INT64"))
                elif col_type == "numeric":
                    return standardize_numeric(node)
                elif col_type == "boolean":
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

    def _build_merge_statement(
        self, retailer_id: int, datetime_threshold: str
    ) -> JSQLNode:
        """Build MERGE statement."""

        # Key columns for merge
        merge_keys = ["retailer_id", "order_id", "product_id", "visitor_id", "order_ts"]

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
                expression=exp.func(
                    "TIMESTAMP", exp.Literal.string(datetime_threshold)
                ),
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
            "session_id": exp.Literal.string(""),
            "crm_id": exp.Null(),
            "platform": exp.Literal.number(1),
            "user_ip": exp.Null(),
            "zip_code": exp.Null(),
            "hashed_email": exp.Null(),
            "hashed_pii": exp.Array(expressions=[]),
            "merchant_id": exp.Null(),
            "child_product_id": exp.Null(),
            "country": exp.Null(),
            "seller_id": exp.Null(),
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
    def simple_dedup(
        partition_by: List[str], order_by: List[Tuple[str, str]] = None
    ) -> str:
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
            if col == "child_product_id":
                # COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))
                partition_exprs.append(
                    "COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))"
                )
            elif col == "hashed_pii":
                # Array serialization
                partition_exprs.append(
                    """
ARRAY_TO_STRING(
    ARRAY(
        SELECT CONCAT(pii.key, ':', pii.value)
        FROM UNNEST(hashed_pii) AS pii
        WHERE pii.key IS NOT NULL AND pii.value IS NOT NULL
        ORDER BY pii.key, pii.value
    ),
    ', '
)""".strip()
                )
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

            if col_type == "string_id":
                expr = standardize_string_id(col_ref)
            elif col_type == "integer":
                expr = exp.Cast(this=col_ref, to=exp.DataType.build("INT64"))
            elif col_type == "numeric":
                expr = standardize_numeric(col_ref)
            elif col_type == "boolean":
                expr = standardize_boolean(col_ref)
            else:
                expr = col_ref

            standardized.append(exp.alias_(expr, col_name))

        return standardized

    @staticmethod
    def apply_to_query(
        query: exp.Expression, columns: Dict[str, str]
    ) -> exp.Expression:
        """Apply standardization to all matching columns in a query."""

        def standardize_node(node):
            if isinstance(node, exp.Column) and node.name in columns:
                col_type = columns[node.name]

                # Don't transform if already in a function
                parent = node.parent
                if isinstance(parent, exp.Func):
                    return node

                if col_type == "string_id":
                    return standardize_string_id(node)
                elif col_type == "integer":
                    return exp.Cast(this=node, to=exp.DataType.build("INT64"))
                elif col_type == "numeric":
                    return standardize_numeric(node)
                elif col_type == "boolean":
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
            "complexity_score": len(tables) + len(joins) + len(subqueries),
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
                this=exp.Column(this="visitor_id", table="vib"), expression=exp.Null()
            ),
            expression=exp.NEQ(this=exp.Column(this="csr_flag"), expression=exp.true()),
        )

    @staticmethod
    def best_buy_product_hierarchy(datetime_threshold: str) -> str:
        """Best Buy's product hierarchy flattening pattern."""

        return f"""
        SELECT
            o.order_id,
            o.visitor_id,
            o.order_timestamp,
            o.sku as product_id,
            -- Flatten product hierarchy
            COALESCE(p.department_id, p.category_id, p.subcategory_id) as product_hierarchy_id,
            p.department_name,
            p.category_name,
            p.subcategory_name,
            o.quantity,
            o.unit_price,
            o.store_id,
            o.online_exclusive_flag,
            o.geek_squad_protection,
            o.my_best_buy_member_flag
        FROM `bestbuy-prod.orders.raw_transactions` o
        LEFT JOIN `bestbuy-prod.products.product_hierarchy` p
            ON o.sku = p.sku
        WHERE o.order_timestamp >= TIMESTAMP('{datetime_threshold}')
            AND o.order_status IN ('COMPLETED', 'SHIPPED')
        """

    @staticmethod
    def target_redcard_handling(datetime_threshold: str) -> str:
        """Target's RedCard discount handling pattern."""

        return f"""
        SELECT
            t.transaction_id as order_id,
            t.guest_id as visitor_id,
            t.transaction_datetime as order_ts,
            t.dpci as product_id,
            t.quantity,
            t.regular_price,
            -- Handle RedCard 5% discount
            CASE
                WHEN t.redcard_flag = TRUE THEN t.regular_price * 0.95
                ELSE t.sale_price
            END as final_price,
            t.redcard_flag,
            t.redcard_discount_amount,
            t.circle_rewards_applied,
            t.circle_earnings,
            t.store_number,
            t.fulfillment_type,
            t.shipt_flag
        FROM `target-prod.transactions.pos_data` t
        WHERE t.transaction_datetime >= TIMESTAMP('{datetime_threshold}')
            AND t.transaction_type = 'SALE'
            AND t.void_flag = FALSE
        """

    @staticmethod
    def walmart_marketplace_consolidation(datetime_threshold: str) -> str:
        """Walmart's marketplace seller consolidation pattern."""

        return f"""
        SELECT
            COALESCE(w.order_id, m.order_id) as order_id,
            COALESCE(w.customer_id, m.customer_id) as visitor_id,
            COALESCE(w.order_date, m.order_date) as order_ts,
            COALESCE(w.item_id, m.seller_item_id) as product_id,
            COALESCE(w.quantity, m.quantity) as quantity,
            COALESCE(w.unit_price, m.seller_price) as price,
            -- Identify source
            CASE
                WHEN w.order_id IS NOT NULL AND m.order_id IS NULL THEN 'WALMART'
                WHEN w.order_id IS NULL AND m.order_id IS NOT NULL THEN 'MARKETPLACE'
                ELSE 'BOTH'
            END as source_system,
            m.seller_id,
            m.seller_name,
            w.store_number,
            w.pickup_flag
        FROM `walmart-prod.orders.walmart_direct` w
        FULL OUTER JOIN `walmart-prod.orders.marketplace_orders` m
            ON w.order_id = m.order_id
            AND w.item_id = m.seller_item_id
        WHERE COALESCE(w.order_date, m.order_date) >= TIMESTAMP('{datetime_threshold}')
        """
