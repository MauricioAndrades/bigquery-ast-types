#!/usr/bin/env python3
"""
Tests for BigQuery Type Support

Comprehensive tests for parsing, validation, and integration
of BigQuery data types.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import unittest
from typing import List, Tuple

from bq_types import (
    BQDataType, BigQueryType, TypeParser, TypeValidator,
    TypeCaster, TypeSize, TypeParameter
)
from type_integration import TypeMapper, TypeInferrer, TableSchema, TypedColumn


class TestTypeParser(unittest.TestCase):
    """Test BigQuery type parsing."""
    
    def test_simple_types(self):
        """Test parsing simple types."""
        test_cases = [
            ("INT64", BQDataType.INT64),
            ("STRING", BQDataType.STRING),
            ("BOOL", BQDataType.BOOL),
            ("FLOAT64", BQDataType.FLOAT64),
            ("DATE", BQDataType.DATE),
            ("TIMESTAMP", BQDataType.TIMESTAMP),
        ]
        
        for type_str, expected in test_cases:
            with self.subTest(type_str=type_str):
                result = TypeParser.parse(type_str)
                self.assertEqual(result.base_type, expected)
                self.assertEqual(len(result.parameters), 0)
    
    def test_type_aliases(self):
        """Test type alias parsing."""
        test_cases = [
            ("INT", BQDataType.INT64),
            ("BIGINT", BQDataType.INT64),
            ("BOOLEAN", BQDataType.BOOL),
            ("DECIMAL", BQDataType.NUMERIC),
            ("BIGDECIMAL", BQDataType.BIGNUMERIC),
        ]
        
        for alias, expected in test_cases:
            with self.subTest(alias=alias):
                result = TypeParser.parse(alias)
                self.assertEqual(result.base_type, expected)
    
    def test_parameterized_types(self):
        """Test parsing parameterized types."""
        # STRING(100)
        result = TypeParser.parse("STRING(100)")
        self.assertEqual(result.base_type, BQDataType.STRING)
        self.assertEqual(len(result.parameters), 1)
        self.assertEqual(result.parameters[0].value, 100)
        
        # NUMERIC(10, 2)
        result = TypeParser.parse("NUMERIC(10, 2)")
        self.assertEqual(result.base_type, BQDataType.NUMERIC)
        self.assertEqual(len(result.parameters), 2)
        self.assertEqual(result.parameters[0].value, 10)
        self.assertEqual(result.parameters[1].value, 2)
    
    def test_array_types(self):
        """Test parsing array types."""
        # Simple array
        result = TypeParser.parse("ARRAY<INT64>")
        self.assertEqual(result.base_type, BQDataType.ARRAY)
        self.assertIsNotNone(result.element_type)
        self.assertEqual(result.element_type.base_type, BQDataType.INT64)
        
        # Array of parameterized type
        result = TypeParser.parse("ARRAY<STRING(50)>")
        self.assertEqual(result.base_type, BQDataType.ARRAY)
        self.assertEqual(result.element_type.base_type, BQDataType.STRING)
        self.assertEqual(result.element_type.parameters[0].value, 50)
    
    def test_struct_types(self):
        """Test parsing struct types."""
        # Simple struct
        result = TypeParser.parse("STRUCT<x INT64, y STRING>")
        self.assertEqual(result.base_type, BQDataType.STRUCT)
        self.assertEqual(len(result.fields), 2)
        
        # Check fields
        self.assertEqual(result.fields[0][0], "x")
        self.assertEqual(result.fields[0][1].base_type, BQDataType.INT64)
        self.assertEqual(result.fields[1][0], "y")
        self.assertEqual(result.fields[1][1].base_type, BQDataType.STRING)
    
    def test_range_types(self):
        """Test parsing range types."""
        result = TypeParser.parse("RANGE<DATE>")
        self.assertEqual(result.base_type, BQDataType.RANGE)
        self.assertIsNotNone(result.range_type)
        self.assertEqual(result.range_type.base_type, BQDataType.DATE)
    
    def test_type_string_representation(self):
        """Test string representation of types."""
        test_cases = [
            ("INT64", "INT64"),
            ("STRING(100)", "STRING(100)"),
            ("NUMERIC(10, 2)", "NUMERIC(10, 2)"),
            ("ARRAY<INT64>", "ARRAY<INT64>"),
            ("STRUCT<x INT64, y STRING>", "STRUCT<x INT64, y STRING>"),
            ("RANGE<DATE>", "RANGE<DATE>"),
        ]
        
        for input_str, expected_str in test_cases:
            with self.subTest(input_str=input_str):
                result = TypeParser.parse(input_str)
                self.assertEqual(str(result), expected_str)


class TestTypeValidator(unittest.TestCase):
    """Test type validation."""
    
    def test_string_parameter_validation(self):
        """Test STRING(L) parameter validation."""
        # Valid
        valid_type = BigQueryType(
            base_type=BQDataType.STRING,
            parameters=[TypeParameter("length", 100)]
        )
        self.assertTrue(TypeValidator.validate_parameterized_type(valid_type))
        
        # Invalid - negative length
        invalid_type = BigQueryType(
            base_type=BQDataType.STRING,
            parameters=[TypeParameter("length", -1)]
        )
        self.assertFalse(TypeValidator.validate_parameterized_type(invalid_type))
    
    def test_numeric_parameter_validation(self):
        """Test NUMERIC(P, S) parameter validation."""
        # Valid NUMERIC(10, 2)
        valid_type = BigQueryType(
            base_type=BQDataType.NUMERIC,
            parameters=[
                TypeParameter("precision", 10),
                TypeParameter("scale", 2)
            ]
        )
        self.assertTrue(TypeValidator.validate_parameterized_type(valid_type))
        
        # Invalid - scale > 9
        invalid_type = BigQueryType(
            base_type=BQDataType.NUMERIC,
            parameters=[
                TypeParameter("precision", 20),
                TypeParameter("scale", 10)
            ]
        )
        self.assertFalse(TypeValidator.validate_parameterized_type(invalid_type))
        
        # Invalid - precision too small
        invalid_type = BigQueryType(
            base_type=BQDataType.NUMERIC,
            parameters=[
                TypeParameter("precision", 2),
                TypeParameter("scale", 5)
            ]
        )
        self.assertFalse(TypeValidator.validate_parameterized_type(invalid_type))
    
    def test_type_properties(self):
        """Test type property checks."""
        # Orderable type
        int_type = BigQueryType(base_type=BQDataType.INT64)
        self.assertTrue(TypeValidator.is_orderable(int_type))
        
        # Non-orderable type
        json_type = BigQueryType(base_type=BQDataType.JSON)
        self.assertFalse(TypeValidator.is_orderable(json_type))
        
        # Comparable type
        string_type = BigQueryType(base_type=BQDataType.STRING)
        self.assertTrue(TypeValidator.is_comparable(string_type))
        
        # Non-comparable type
        geography_type = BigQueryType(base_type=BQDataType.GEOGRAPHY)
        self.assertFalse(TypeValidator.is_comparable(geography_type))


class TestTypeCaster(unittest.TestCase):
    """Test type casting utilities."""
    
    def test_implicit_cast_rules(self):
        """Test implicit cast checking."""
        # INT64 -> FLOAT64 (allowed)
        self.assertTrue(TypeCaster.can_implicit_cast(BQDataType.INT64, BQDataType.FLOAT64))
        
        # FLOAT64 -> INT64 (not allowed)
        self.assertFalse(TypeCaster.can_implicit_cast(BQDataType.FLOAT64, BQDataType.INT64))
        
        # Same type (always allowed)
        self.assertTrue(TypeCaster.can_implicit_cast(BQDataType.STRING, BQDataType.STRING))
    
    def test_common_supertype(self):
        """Test finding common supertype."""
        # Numeric types
        types = [BQDataType.INT64, BQDataType.FLOAT64]
        result = TypeCaster.find_common_supertype(types)
        self.assertEqual(result, BQDataType.FLOAT64)
        
        # All same type
        types = [BQDataType.STRING, BQDataType.STRING]
        result = TypeCaster.find_common_supertype(types)
        self.assertEqual(result, BQDataType.STRING)
        
        # No common supertype
        types = [BQDataType.STRING, BQDataType.INT64]
        result = TypeCaster.find_common_supertype(types)
        self.assertIsNone(result)


class TestTypeSize(unittest.TestCase):
    """Test type size calculations."""
    
    def test_fixed_size_types(self):
        """Test fixed size types."""
        test_cases = [
            (BQDataType.BOOL, 1),
            (BQDataType.INT64, 8),
            (BQDataType.FLOAT64, 8),
            (BQDataType.DATE, 8),
            (BQDataType.TIMESTAMP, 8),
            (BQDataType.NUMERIC, 16),
            (BQDataType.BIGNUMERIC, 32),
        ]
        
        for data_type, expected_size in test_cases:
            with self.subTest(data_type=data_type):
                size_info = TypeSize.get_size(data_type)
                self.assertEqual(size_info.fixed_size, expected_size)
    
    def test_variable_size_types(self):
        """Test variable size types."""
        # STRING has 2 byte base + UTF-8 content
        size_info = TypeSize.get_size(BQDataType.STRING)
        self.assertIsNone(size_info.fixed_size)
        self.assertEqual(size_info.variable_base, 2)
        
        # STRUCT has 0 byte base + field sizes
        size_info = TypeSize.get_size(BQDataType.STRUCT)
        self.assertIsNone(size_info.fixed_size)
        self.assertEqual(size_info.variable_base, 0)


class TestTypeIntegration(unittest.TestCase):
    """Test type integration with SQL AST."""
    
    def setUp(self):
        """Set up test schemas."""
        self.schema = TableSchema(
            name="test_table",
            columns=[
                TypedColumn("id", BigQueryType(base_type=BQDataType.INT64), nullable=False),
                TypedColumn("name", BigQueryType(base_type=BQDataType.STRING)),
                TypedColumn("amount", BigQueryType(base_type=BQDataType.NUMERIC)),
                TypedColumn("created", BigQueryType(base_type=BQDataType.TIMESTAMP)),
            ]
        )
        self.schemas = {"test_table": self.schema}
        self.inferrer = TypeInferrer(self.schemas)
    
    def test_literal_type_inference(self):
        """Test inferring types of literals."""
        from sqlglot import parse_one
        
        # Integer literal
        expr = parse_one("SELECT 42").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.INT64)
        
        # String literal
        expr = parse_one("SELECT 'hello'").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.STRING)
        
        # Boolean literal
        expr = parse_one("SELECT TRUE").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.BOOL)
    
    def test_column_type_inference(self):
        """Test inferring types of column references."""
        from sqlglot import parse_one
        
        # Column with known type
        expr = parse_one("SELECT id FROM test_table").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.INT64)
        
        # Column with table qualifier
        expr = parse_one("SELECT test_table.name FROM test_table").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.STRING)
    
    def test_expression_type_inference(self):
        """Test inferring types of complex expressions."""
        from sqlglot import parse_one
        
        # Arithmetic expression
        expr = parse_one("SELECT id + 10").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.INT64)
        
        # Comparison expression
        expr = parse_one("SELECT id > 10").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.BOOL)
        
        # Function expression
        expr = parse_one("SELECT LENGTH(name)").expressions[0]
        result = self.inferrer.infer_expression_type(expr)
        self.assertEqual(result.base_type, BQDataType.INT64)


def run_all_tests():
    """Run all tests and print summary."""
    print("=== Running BigQuery Type Tests ===\n")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestTypeParser,
        TestTypeValidator,
        TestTypeCaster,
        TestTypeSize,
        TestTypeIntegration,
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✅ All tests passed! BigQuery type support is working correctly! 🐕")
    else:
        print(f"❌ {len(result.failures)} failures, {len(result.errors)} errors")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_all_tests()