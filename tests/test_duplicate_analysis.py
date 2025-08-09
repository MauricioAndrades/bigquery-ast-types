#!/usr/bin/env python3
"""
Test Duplicate Analysis Query Generation

Verifies that we can generate the exact duplicate analysis query pattern.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from utils.symbo_query import DuplicateAnalysisBuilder
except ImportError:
    pytest.skip("DuplicateAnalysisBuilder not available (missing sqlglot)", allow_module_level=True)


def test_duplicate_analysis():
    """Test that we generate the exact query."""

    builder = DuplicateAnalysisBuilder("symbiosys-prod", "event")

    result = builder.build_duplicate_analysis(
        start_date="2025-07-01 00:00:00", end_date="2025-08-01 00:00:00"
    )

    print("Generated Query:")
    print("=" * 60)
    print(result)
    print("=" * 60)

    # Verify key components
    assert "-- Check for duplicates in staging" in result
    assert "WITH deduped AS" in result
    assert "ROW_NUMBER() OVER" in result
    assert "PARTITION BY" in result
    assert (
        "COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING))"
        in result
    )
    assert "ARRAY_TO_STRING(" in result
    assert "FROM UNNEST(hashed_pii) AS pii" in result
    assert "WHERE pii.key   IS NOT NULL" in result  # Note the exact spacing
    assert "AND pii.value IS NOT NULL" in result
    assert "ORDER BY pii.key, pii.value" in result
    assert "ORDER BY duplicate_percentage DESC" in result
    assert "COUNTIF(rn > 1)" in result
    assert "ROUND(COUNTIF(rn > 1) * 100.0 / COUNT(*), 2)" in result

    print("\n✅ All assertions passed!")


if __name__ == "__main__":
    test_duplicate_analysis()
