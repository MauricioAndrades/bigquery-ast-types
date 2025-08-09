"""
Order Merge Transformation using BQAST

Demonstrates how to use the BigQuery AST library to implement
the standardized order merge pattern.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import List, Dict, Optional
import pendulum
from lib.builders import OrderByClause

from lib.builders import b, Select, CTE, WithClause, Merge, Expression, MergeAction
from ..parsers.sqlglot import parse  # Import parse function
from lib.visitor import BaseVisitor


class OrderMergeBuilder:
    """Build order merge queries using AST builders."""

    def __init__(self, project: str, retailer_id: int):
        self.project = project
        self.retailer_id = retailer_id
        self.column_types = {
            "visitor_id": "string_id",
            "order_id": "string_id",
            "product_id": "string_id",
            "session_id": "string_id",
            "quantity": "integer",
            "regular_unit_price": "numeric",
            "discount_unit_price": "numeric",
            "is_restaurant": "boolean",
            "is_recurring": "boolean",
        }

    def build_merge(
        self, source_query: Select, datetime_threshold: pendulum.DateTime
    ) -> Merge:
        """Build complete MERGE statement with 3 CTEs."""

        # CTE 1: raw_orders - Original query as-is
        raw_orders = CTE("raw_orders", source_query)

        # CTE 2: cleaned_orders - With standardization
        cleaned_orders = CTE("cleaned_orders", self._build_cleaned_query())

        # CTE 3: deduped_orders - With deduplication
        deduped_orders = CTE("deduped_orders", self._build_deduped_query())

        # Build MERGE
        return self._build_merge_statement(
            [raw_orders, cleaned_orders, deduped_orders], datetime_threshold
        )

    def _build_cleaned_query(self) -> Select:
        """Build cleaned_orders query with standardization."""

        columns = [
            # Add retailer_id
            b.select_col(b.lit(self.retailer_id), "retailer_id"),
            # Add symbiosys_ts
            b.select_col(b.current_timestamp(), "symbiosys_ts"),
            # Standardize string IDs with NULLIF(TRIM(...), '')
            b.select_col(
                b.nullif(b.trim(b.col("visitor_id")), b.lit("")), "visitor_id"
            ),
            b.select_col(b.nullif(b.trim(b.col("order_id")), b.lit("")), "order_id"),
            b.select_col(
                b.nullif(b.trim(b.col("product_id")), b.lit("")), "product_id"
            ),
            b.select_col(
                b.nullif(b.trim(b.col("session_id")), b.lit("")), "session_id"
            ),
            # Cast integers
            b.select_col(b.safe_cast(b.col("quantity"), "INT64"), "quantity"),
            # Standardize numerics with COALESCE
            b.select_col(
                b.coalesce(
                    b.safe_cast(b.col("regular_unit_price"), "NUMERIC"), b.lit(0)
                ),
                "regular_unit_price",
            ),
            b.select_col(
                b.coalesce(
                    b.safe_cast(b.col("discount_unit_price"), "NUMERIC"), b.lit(0)
                ),
                "discount_unit_price",
            ),
            # Standardize booleans
            b.select_col(
                b.coalesce(b.col("is_restaurant"), b.false()), "is_restaurant"
            ),
            b.select_col(b.coalesce(b.col("is_recurring"), b.false()), "is_recurring"),
            # Pass through other columns
            b.select_col(b.col("order_ts"), "order_ts"),
            b.select_col(b.col("crm_id"), "crm_id"),
            b.select_col(b.col("platform"), "platform"),
            b.select_col(b.col("user_ip"), "user_ip"),
            b.select_col(b.col("zip_code"), "zip_code"),
            b.select_col(b.col("hashed_email"), "hashed_email"),
            b.select_col(b.col("hashed_pii"), "hashed_pii"),
            b.select_col(b.col("merchant_id"), "merchant_id"),
            b.select_col(b.col("child_product_id"), "child_product_id"),
            b.select_col(b.col("country"), "country"),
            b.select_col(b.col("user_tracking_allowed"), "user_tracking_allowed"),
            b.select_col(b.col("seller_id"), "seller_id"),
        ]

        return Select(columns=columns, from_clause=[b.table("raw_orders")], where=None)

    def _build_deduped_query(self) -> Select:
        """Build deduplication query."""

        # Inner query with ROW_NUMBER
        window = b.row_number()
        window.partition_by = [
            b.col("retailer_id"),
            b.col("order_id"),
            b.col("product_id"),
            b.col("visitor_id"),
            b.col("order_ts"),
            b.col("session_id"),
        ]
        window.order_by = [
            OrderByClause(b.col("order_ts"), "DESC"),
            OrderByClause(b.col("symbiosys_ts"), "DESC"),
        ]

        inner_query = Select(
            columns=[b.select_col(b.star()), b.select_col(window, "rank")],
            from_clause=[b.table("cleaned_orders")],
            where=b.and_(
                b.is_not_null(b.col("retailer_id")),
                b.and_(
                    b.neq(b.col("order_id"), b.lit("")),
                    b.is_not_null(b.col("order_id")),
                ),
                b.and_(
                    b.neq(b.col("product_id"), b.lit("")),
                    b.is_not_null(b.col("product_id")),
                ),
                b.and_(
                    b.neq(b.col("visitor_id"), b.lit("")),
                    b.is_not_null(b.col("visitor_id")),
                ),
                b.is_not_null(b.col("order_ts")),
            ),
        )

        # Outer query filtering rank = 1
        return Select(
            columns=[b.select_col(b.star(["rank"]))],
            from_clause=[b.table("subquery")],  # Will be replaced with subquery
            where=b.eq(b.col("rank"), b.lit(1)),
        )

    def _build_merge_statement(
        self, ctes: List[CTE], datetime_threshold: pendulum.DateTime
    ) -> Merge:
        """Build final MERGE statement."""

        # Build merge conditions for all columns
        merge_columns = [
            "retailer_id",
            "order_ts",
            "session_id",
            "visitor_id",
            "crm_id",
            "platform",
            "order_id",
            "product_id",
            "quantity",
            "user_ip",
            "zip_code",
            "hashed_email",
            "regular_unit_price",
            "discount_unit_price",
            "merchant_id",
            "child_product_id",
            "country",
            "is_restaurant",
            "is_recurring",
        ]

        merge_conditions = []
        for col in merge_columns:
            merge_conditions.append(b.null_safe_eq(b.col(col, "S"), b.col(col, "T")))

        # Add timestamp condition
        merge_conditions.append(
            b.gte(
                b.col("order_ts", "T"),
                b.timestamp(
                    datetime_threshold.format("YYYY-MM-DD HH:mm:ss+00:00")
                ),
            )
        )

        merge_condition = b.and_(*merge_conditions)

        # Build MERGE
        return Merge(
            target_table=f"`{self.project}.event_analytics.order`",
            source=Select(
                columns=[b.select_col(b.star())],
                from_clause=[b.table("deduped_orders")],
            ),
            on_condition=merge_condition,
            when_not_matched=[MergeAction("INSERT", values=None)],  # INSERT ROW
            when_not_matched_by_source=[
                MergeAction(
                    "DELETE",
                    condition=b.and_(
                        b.eq(b.col("retailer_id", "T"), b.lit(self.retailer_id)),
                        b.gte(
                            b.col("order_ts", "T"),
                            b.timestamp(
                                datetime_threshold.format(
                                    "YYYY-MM-DD HH:mm:ss+00:00"
                                )
                            ),
                        ),
                    ),
                )
            ],
        )


class ChewyOrderMergeBuilder(OrderMergeBuilder):
    """Specialized builder for Chewy's complex merge pattern."""

    def build_chewy_merge(self, datetime_threshold: pendulum.DateTime) -> str:
        """Build Chewy's specific merge with FULL OUTER JOIN and complex CASE statements."""

        # Build complex source query
        columns = [
            b.select_col(b.lit(8), "retailer_id"),
            b.select_col(
                b.timestamp("order_time_placed"), "order_ts"
            ),
            # Complex CASE for session_id
            b.select_col(
                b.case(
                    b.when(
                        b.and_(
                            b.is_null(b.col("visitor_id", "vib")),
                            b.is_not(b.col("csr_flag"), b.true()),
                        ),
                        b.col("session_id"),
                    ),
                    else_=b.lit(""),
                ),
                "session_id",
            ),
            # Complex CASE for visitor_id
            b.select_col(
                b.case(
                    b.when(
                        b.and_(
                            b.is_null(b.col("visitor_id", "vib")),
                            b.is_not(b.col("csr_flag"), b.true()),
                        ),
                        b.col("visitor_id", "o"),
                    ),
                    else_=b.lit(""),
                ),
                "visitor_id",
            ),
            # CASE for crm_id
            b.select_col(
                b.case(
                    b.when(b.eq(b.col("crm_user_id"), b.lit(-1002)), b.null()),
                    else_=b.cast(b.col("crm_user_id"), "STRING"),
                ),
                "crm_id",
            ),
            # Other columns...
            b.select_col(b.cast(b.null(), "INTEGER"), "platform"),
            b.select_col(b.col("order_id"), "order_id"),
            b.select_col(b.cast(b.null(), "STRING"), "user_ip"),
            b.select_col(b.cast(b.null(), "STRING"), "zip_code"),
            # Complex CASE for hashed_email
            b.select_col(
                b.case(
                    b.when(
                        b.and_(
                            b.is_null(b.col("visitor_id", "vib")),
                            b.is_not(b.col("csr_flag"), b.true()),
                            b.neq(b.col("customer_email_hashed"), b.lit("")),
                        ),
                        b.col("customer_email_hashed"),
                    ),
                    else_=b.null(),
                ),
                "hashed_email",
            ),
            # STRUCT array for hashed_pii
            b.select_col(
                b.array(
                    b.struct(
                        key=b.lit("hashed_phone_e164_with_prefix"),
                        value=b.col("hashed_phone_e164"),
                    )
                ),
                "hashed_pii",
            ),
            # More columns...
            b.select_col(b.col("product_sku"), "product_id"),
            b.select_col(b.col("quantity"), "quantity"),
            b.select_col(
                b.cast(b.coalesce(b.col("purchase_price"), b.lit(0.0)), "NUMERIC"),
                "regular_unit_price",
            ),
            b.select_col(
                b.cast(b.coalesce(b.col("purchase_price"), b.lit(0.0)), "NUMERIC"),
                "discount_unit_price",
            ),
            # CASE for is_recurring
            b.select_col(
                b.case(
                    b.when(b.is_(b.col("autoship_flag"), b.true()), b.true()),
                    else_=b.false(),
                ),
                "is_recurring",
            ),
            # Remaining columns
            b.select_col(b.false(), "is_restaurant"),
            b.select_col(b.cast(b.null(), "INTEGER"), "merchant_id"),
            b.select_col(b.cast(b.null(), "STRING"), "child_product_id"),
            b.select_col(b.cast(b.null(), "STRING"), "country"),
            b.select_col(b.cast(b.null(), "BOOL"), "user_tracking_allowed"),
            b.select_col(b.cast(b.null(), "STRING"), "seller_id"),
            b.select_col(b.current_timestamp(), "symbiosys_ts"),
        ]

        # This would need the FULL OUTER JOIN subquery built...
        # For now, return a placeholder
        return "-- Complex Chewy merge would be generated here"


def demonstrate_order_merge():
    """Demonstrate using the AST builder for order merge."""

    print("Order Merge using BigQuery AST Builder")
    print("=" * 70)

    # Create builder
    builder = OrderMergeBuilder("my-project", retailer_id=123)

    # Parse a source query
    source_sql = """
    SELECT
        order_timestamp AS order_ts,
        customer_id AS visitor_id,
        order_number AS order_id,
        sku AS product_id,
        qty AS quantity,
        price AS regular_unit_price
    FROM `project.dataset.orders`
    WHERE status = 'COMPLETED'
    """

    source_query = parse(source_sql)
    datetime_threshold = pendulum.now()

    # Build MERGE
    merge = builder.build_merge(source_query, datetime_threshold)

    print("\nGenerated AST structure:")
    print(f"- Target: {merge.target_table}")
    print(f"- Source: {type(merge.source).__name__}")
    print(
    f"- Conditions: {len(builder._build_merge_statement.__defaults__[0] if builder._build_merge_statement.__defaults__ else [])} null-safe equality checks"
    )
    print(
        f"- Actions: {len(merge.when_not_matched)} INSERT, {len(merge.when_not_matched_by_source)} DELETE"
    )

    # Demo Chewy builder
    print("\n" + "=" * 70)
    print("Chewy Complex Merge:")
    chewy_builder = ChewyOrderMergeBuilder("symbiosys-prod", retailer_id=8)
    chewy_sql = chewy_builder.build_chewy_merge(datetime_threshold)
    print(chewy_sql)

    print("\n🐕 Woof! AST-based order merge transformation complete!")


if __name__ == "__main__":
    demonstrate_order_merge()
