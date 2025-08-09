#!/usr/bin/env python3
"""
Demo script to showcase BigQuery ML and External Table functionality.
"""

import sys
import os

sys.path.append(os.path.dirname(__file__))

from lib import b
from lib.types import Select
from lib.serializer import to_sql

def demo_ml_features():
    """Demo BigQuery ML features."""
    print("=== BigQuery ML Features Demo ===\n")
    
    # 1. CREATE MODEL
    print("1. CREATE MODEL:")
    model = b.create_model(
        "my_dataset.customer_churn_model",
        options={
            "model_type": "logistic_reg",
            "input_label_cols": ["churned"],
            "max_iterations": 50
        }
    )
    print(to_sql(model))
    print()
    
    # 2. ML.PREDICT
    print("2. ML.PREDICT:")
    predict = b.ml_predict(
        "my_dataset.customer_churn_model",
        b.table("my_dataset.customer_data"),
        struct_options={"threshold": 0.7}
    )
    print(to_sql(predict))
    print()
    
    # 3. ML.EVALUATE
    print("3. ML.EVALUATE:")
    evaluate = b.ml_predict(  # Using ml_predict as a placeholder for evaluate functionality
        "my_dataset.customer_churn_model", 
        b.table("my_dataset.test_data")
    )
    print(to_sql(evaluate))
    print()

def demo_external_tables():
    """Demo External Table features."""
    print("=== External Table Features Demo ===\n")
    
    # 1. CREATE EXTERNAL TABLE
    print("1. CREATE EXTERNAL TABLE:")
    ext_table = b.create_external_table(
        "my_dataset.sales_data_external",
        schema=["sale_id INT64", "product_name STRING", "amount FLOAT64", "sale_date DATE"],
        options={
            "format": "CSV",
            "uris": ["gs://my-data-bucket/sales/*.csv"],
            "skip_leading_rows": 1,
            "allow_jagged_rows": False
        }
    )
    print(to_sql(ext_table))
    print()
    
    # 2. EXPORT DATA
    print("2. EXPORT DATA:")
    query = Select(select_list=[
        b.select_col(b.col("product_name")),
        b.select_col(b.col("SUM(amount)"), "total_sales")
    ])
    query.from_clause = b.table("my_dataset.sales_data")
    query.group_by_clause = b.group_by(b.col("product_name"))
    
    export = b.export_data(
        options={
            "uri": "gs://my-export-bucket/sales_summary-*.csv",
            "format": "CSV",
            "overwrite": True,
            "header": True
        },
        as_query=query
    )
    print(to_sql(export))
    print()

def main():
    """Run the demo."""
    print("BigQuery ML and External Table AST Demo")
    print("=" * 50)
    print()
    
    demo_ml_features()
    demo_external_tables()
    
    print("Demo completed successfully!")

if __name__ == "__main__":
    main()