"""
Comprehensive MERGE Builder for BigQuery

Generates MERGE DELETE statements with full column comparison
including array handling and special normalizations.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
from typing import List, Dict, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "sqlglot"))

import sqlglot
from sqlglot import exp
from lib.bsql import j, null_safe_eq


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


def test_comprehensive_merge():
    """Test the comprehensive merge builder."""

    builder = ComprehensiveMergeBuilder("symbiosys-prod", "event_analytics")

    result = builder.build_comprehensive_merge(
        start_date="2025-07-01 00:00:00",
        end_date="2025-08-01 00:00:00",
        filter_start="2025-07-27 00:00:00",
        filter_end="2025-07-30 00:00:00"
    )

    print("Generated Comprehensive MERGE:")
    print("=" * 80)
    print(result)
    print("=" * 80)

    # Verify key components
    assert "MERGE `event_analytics.order` AS T USING" in result
    assert "WITH deduped AS" in result
    assert "COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))" in result
    assert "ARRAY_TO_STRING(" in result
    assert "WHERE pii.key   IS NOT NULL" in result  # Exact spacing
    assert "-- inline, sorted fingerprint of the PII array" in result
    assert "WHEN MATCHED THEN\n  DELETE;" in result
    assert "T.retailer_id = B.retailer_id" in result
    assert "(T.order_id = B.order_id OR (T.order_id IS NULL AND B.order_id IS NULL))" in result

    print("\n✅ All assertions passed!")


if __name__ == "__main__":
    test_comprehensive_merge()