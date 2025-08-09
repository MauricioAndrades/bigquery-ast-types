#!/usr/bin/env python3
"""
Test BigQuery ML and External Table AST Support

Focused tests for the new BigQuery ML and External Table functionality.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os

sys.path.append(os.path.dirname(__file__))

from lib import b, ValidationError
from lib.types import (
    CreateModel, MLPredict, MLEvaluate, MLExplain,
    CreateExternalTable, ExportData, LoadData,
    Select, TableRef
)
from lib.serializer import to_sql


def test_create_model():
    """Test CREATE MODEL builder and serialization."""
    print("Testing CREATE MODEL...")
    
    # Basic model creation
    model = b.create_model("my_dataset.my_model")
    assert isinstance(model, CreateModel)
    assert model.model_name == "my_dataset.my_model"
    assert model.options == {}
    assert model.as_query is None
    assert model.transform is None
    
    # Model with options
    model_with_options = b.create_model(
        "my_dataset.regression_model",
        options={
            "model_type": "linear_reg",
            "input_label_cols": ["label"],
            "max_iterations": 50
        }
    )
    assert len(model_with_options.options) == 3
    
    # Model with query
    from lib.types import Select
    query = Select(select_list=[
        b.select_col(b.col("feature1")), 
        b.select_col(b.col("label"))
    ])
    query.from_clause = b.table("training_data")
    
    model_with_query = b.create_model(
        "my_dataset.model_with_data",
        as_query=query
    )
    assert model_with_query.as_query is not None
    
    # Test serialization
    sql = to_sql(model)
    assert "CREATE MODEL my_dataset.my_model" in sql
    
    print("  ✓ CREATE MODEL tests passed!")


def test_ml_predict():
    """Test ML.PREDICT builder and serialization."""
    print("Testing ML.PREDICT...")
    
    # Basic prediction with table
    table_ref = b.table("input_data")
    predict = b.ml_predict("my_dataset.my_model", table_ref)
    assert isinstance(predict, MLPredict)
    assert predict.model_name == "my_dataset.my_model"
    assert predict.input_data == table_ref
    assert predict.struct_options == {}
    
    # Prediction with SELECT query
    from lib.types import Select
    query = Select(select_list=[
        b.select_col(b.col("feature1")), 
        b.select_col(b.col("feature2"))
    ])
    query.from_clause = b.table("new_data")
    
    predict_with_query = b.ml_predict("my_dataset.my_model", query)
    assert isinstance(predict_with_query.input_data, Select)
    
    # Prediction with options
    predict_with_options = b.ml_predict(
        "my_dataset.my_model",
        table_ref,
        struct_options={"threshold": 0.5}
    )
    assert len(predict_with_options.struct_options) == 1
    
    # Test serialization
    sql = to_sql(predict)
    assert "ML.PREDICT(MODEL my_dataset.my_model" in sql
    
    print("  ✓ ML.PREDICT tests passed!")


def test_create_external_table():
    """Test CREATE EXTERNAL TABLE builder and serialization."""
    print("Testing CREATE EXTERNAL TABLE...")
    
    # Basic external table
    ext_table = b.create_external_table("my_dataset.external_table")
    assert isinstance(ext_table, CreateExternalTable)
    assert ext_table.table_name == "my_dataset.external_table"
    assert ext_table.schema == []
    assert ext_table.options == {}
    
    # External table with schema and options
    ext_table_full = b.create_external_table(
        "my_dataset.csv_table",
        schema=["id INT64", "name STRING", "age INT64"],
        options={
            "format": "CSV",
            "uris": ["gs://my-bucket/data.csv"],
            "skip_leading_rows": 1
        }
    )
    assert len(ext_table_full.schema) == 3
    assert len(ext_table_full.options) == 3
    
    # Test serialization
    sql = to_sql(ext_table)
    assert "CREATE OR REPLACE EXTERNAL TABLE my_dataset.external_table" in sql
    
    print("  ✓ CREATE EXTERNAL TABLE tests passed!")


def test_export_data():
    """Test EXPORT DATA builder and serialization."""
    print("Testing EXPORT DATA...")
    
    # Create a SELECT query for export
    from lib.types import Select
    query = Select(select_list=[b.select_col(b.col("*"))])
    query.from_clause = b.table("my_dataset.my_table")
    
    export = b.export_data(
        options={"uri": "gs://my-bucket/export-*.csv", "format": "CSV"},
        as_query=query
    )
    assert isinstance(export, ExportData)
    assert len(export.options) == 2
    assert export.as_query == query
    
    # Test serialization
    sql = to_sql(export)
    assert "EXPORT DATA" in sql
    assert "AS SELECT" in sql
    
    print("  ✓ EXPORT DATA tests passed!")


def test_validation_errors():
    """Test validation in builders."""
    print("Testing validation errors...")
    
    # Test empty model name
    try:
        b.create_model("")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        assert "Model name must be a non-empty string" in str(e)
    
    # Test invalid model name type
    try:
        b.create_model(None)
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        assert "Model name must be a non-empty string" in str(e)
    
    # Test invalid input data for ML.PREDICT
    try:
        b.ml_predict("model", "invalid_input")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        assert "Input data must be Select or TableRef" in str(e)
    
    # Test empty export options
    from lib.types import Select
    query = Select(select_list=[b.select_col(b.col("*"))])
    query.from_clause = b.table("table")
    
    try:
        b.export_data({}, query)
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        assert "Export options must be a non-empty dictionary" in str(e)
    
    print("  ✓ Validation error tests passed!")


def test_visitor_pattern():
    """Test that all new nodes work with the visitor pattern."""
    print("Testing visitor pattern...")
    
    # Create instances of all new nodes
    model = b.create_model("test.model")
    predict = b.ml_predict("test.model", b.table("data"))
    ext_table = b.create_external_table("test.external")
    
    query = Select(select_list=[b.select_col(b.col("*"))])
    query.from_clause = b.table("table")
    export = b.export_data({"uri": "gs://bucket/file"}, query)
    
    # Test that they all have accept methods
    assert hasattr(model, 'accept')
    assert hasattr(predict, 'accept')
    assert hasattr(ext_table, 'accept')
    assert hasattr(export, 'accept')
    
    # Test that they can all be serialized (uses visitor pattern)
    for node in [model, predict, ext_table, export]:
        sql = to_sql(node)
        assert isinstance(sql, str)
        assert len(sql) > 0
    
    print("  ✓ Visitor pattern tests passed!")


def main():
    """Run all tests."""
    print("============================================================")
    print("BIGQUERY ML AND EXTERNAL TABLE TESTS")
    print("============================================================")
    
    try:
        test_create_model()
        test_ml_predict()
        test_create_external_table()
        test_export_data()
        test_validation_errors()
        test_visitor_pattern()
        
        print("============================================================")
        print("✅ ALL ML AND EXTERNAL TABLE TESTS PASSED!")
        print("============================================================")
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())