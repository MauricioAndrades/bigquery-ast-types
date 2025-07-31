#!/usr/bin/env python3
"""
BigQuery Data Types Support

Parses and understands BigQuery data types based on the official documentation.
Provides utilities for type validation, conversion, and AST integration.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import Optional, List, Dict, Union, Any, Tuple
from enum import Enum
from dataclasses import dataclass, field
import re


class BQDataType(Enum):
    """BigQuery base data types."""
    # Scalar types
    INT64 = "INT64"
    FLOAT64 = "FLOAT64"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    BOOL = "BOOL"
    STRING = "STRING"
    BYTES = "BYTES"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    GEOGRAPHY = "GEOGRAPHY"
    INTERVAL = "INTERVAL"
    JSON = "JSON"
    
    # Complex types
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    RANGE = "RANGE"


# Type aliases mapping
TYPE_ALIASES = {
    "INT": BQDataType.INT64,
    "SMALLINT": BQDataType.INT64,
    "INTEGER": BQDataType.INT64,
    "BIGINT": BQDataType.INT64,
    "TINYINT": BQDataType.INT64,
    "BYTEINT": BQDataType.INT64,
    "BOOLEAN": BQDataType.BOOL,
    "DECIMAL": BQDataType.NUMERIC,
    "BIGDECIMAL": BQDataType.BIGNUMERIC,
}


@dataclass
class TypeParameter:
    """Parameter for parameterized types."""
    name: str
    value: Union[int, str]


@dataclass
class BigQueryType:
    """Complete BigQuery type with parameters and nested types."""
    base_type: BQDataType
    parameters: List[TypeParameter] = field(default_factory=list)
    element_type: Optional['BigQueryType'] = None  # For ARRAY
    fields: List[Tuple[Optional[str], 'BigQueryType']] = field(default_factory=list)  # For STRUCT
    range_type: Optional['BigQueryType'] = None  # For RANGE
    
    def __str__(self) -> str:
        """String representation of the type."""
        if self.base_type == BQDataType.ARRAY:
            return f"ARRAY<{self.element_type}>"
        elif self.base_type == BQDataType.STRUCT:
            field_strs = []
            for name, type_ in self.fields:
                if name:
                    field_strs.append(f"{name} {type_}")
                else:
                    field_strs.append(str(type_))
            return f"STRUCT<{', '.join(field_strs)}>"
        elif self.base_type == BQDataType.RANGE:
            return f"RANGE<{self.range_type}>"
        elif self.parameters:
            param_strs = [str(p.value) for p in self.parameters]
            return f"{self.base_type.value}({', '.join(param_strs)})"
        else:
            return self.base_type.value


class TypeProperties:
    """Properties and constraints for BigQuery types."""
    
    # Nullable types (all types are nullable in BigQuery)
    NULLABLE_TYPES = set(BQDataType)
    
    # Orderable types (can be used in ORDER BY)
    ORDERABLE_TYPES = {
        BQDataType.INT64, BQDataType.FLOAT64, BQDataType.NUMERIC, 
        BQDataType.BIGNUMERIC, BQDataType.BOOL, BQDataType.STRING,
        BQDataType.BYTES, BQDataType.DATE, BQDataType.TIME,
        BQDataType.DATETIME, BQDataType.TIMESTAMP, BQDataType.INTERVAL,
        BQDataType.RANGE
    }
    
    # Groupable types (can be used in GROUP BY, DISTINCT, PARTITION BY)
    GROUPABLE_TYPES = {
        BQDataType.INT64, BQDataType.FLOAT64, BQDataType.NUMERIC,
        BQDataType.BIGNUMERIC, BQDataType.BOOL, BQDataType.STRING,
        BQDataType.BYTES, BQDataType.DATE, BQDataType.TIME,
        BQDataType.DATETIME, BQDataType.TIMESTAMP, BQDataType.INTERVAL,
        BQDataType.ARRAY, BQDataType.STRUCT, BQDataType.RANGE
    }
    
    # Comparable types (can be compared with =, <, >, etc.)
    COMPARABLE_TYPES = {
        BQDataType.INT64, BQDataType.FLOAT64, BQDataType.NUMERIC,
        BQDataType.BIGNUMERIC, BQDataType.BOOL, BQDataType.STRING,
        BQDataType.BYTES, BQDataType.DATE, BQDataType.TIME,
        BQDataType.DATETIME, BQDataType.TIMESTAMP, BQDataType.INTERVAL,
        BQDataType.STRUCT, BQDataType.RANGE
    }
    
    # Collatable types (support collation)
    COLLATABLE_TYPES = {BQDataType.STRING}
    
    # Types that support parameters
    PARAMETERIZABLE_TYPES = {
        BQDataType.STRING,      # STRING(L)
        BQDataType.BYTES,       # BYTES(L)
        BQDataType.NUMERIC,     # NUMERIC(P, S)
        BQDataType.BIGNUMERIC,  # BIGNUMERIC(P, S)
    }


@dataclass
class TypeSize:
    """Size information for BigQuery types in logical bytes."""
    type_: BQDataType
    fixed_size: Optional[int] = None
    variable_base: Optional[int] = None
    
    @staticmethod
    def get_size(type_: BQDataType) -> 'TypeSize':
        """Get size information for a type."""
        sizes = {
            BQDataType.BOOL: TypeSize(BQDataType.BOOL, fixed_size=1),
            BQDataType.INT64: TypeSize(BQDataType.INT64, fixed_size=8),
            BQDataType.FLOAT64: TypeSize(BQDataType.FLOAT64, fixed_size=8),
            BQDataType.NUMERIC: TypeSize(BQDataType.NUMERIC, fixed_size=16),
            BQDataType.BIGNUMERIC: TypeSize(BQDataType.BIGNUMERIC, fixed_size=32),
            BQDataType.DATE: TypeSize(BQDataType.DATE, fixed_size=8),
            BQDataType.TIME: TypeSize(BQDataType.TIME, fixed_size=8),
            BQDataType.DATETIME: TypeSize(BQDataType.DATETIME, fixed_size=8),
            BQDataType.TIMESTAMP: TypeSize(BQDataType.TIMESTAMP, fixed_size=8),
            BQDataType.INTERVAL: TypeSize(BQDataType.INTERVAL, fixed_size=16),
            BQDataType.RANGE: TypeSize(BQDataType.RANGE, fixed_size=16),
            BQDataType.STRING: TypeSize(BQDataType.STRING, variable_base=2),  # 2 + UTF-8 size
            BQDataType.BYTES: TypeSize(BQDataType.BYTES, variable_base=2),    # 2 + byte count
            BQDataType.GEOGRAPHY: TypeSize(BQDataType.GEOGRAPHY, variable_base=16),  # 16 + 24 * vertices
            BQDataType.STRUCT: TypeSize(BQDataType.STRUCT, variable_base=0),  # 0 + field sizes
        }
        return sizes.get(type_, TypeSize(type_))


class TypeParser:
    """Parser for BigQuery type expressions."""
    
    @staticmethod
    def parse(type_str: str) -> BigQueryType:
        """Parse a BigQuery type string into a BigQueryType object."""
        type_str = type_str.strip()
        
        # Handle parameterized types with < >
        if '<' in type_str and type_str.endswith('>'):
            base_type_str = type_str[:type_str.index('<')]
            inner_str = type_str[type_str.index('<')+1:-1]
            
            # Get base type
            base_type = TypeParser._get_base_type(base_type_str)
            
            if base_type == BQDataType.ARRAY:
                # Parse element type
                element_type = TypeParser.parse(inner_str)
                return BigQueryType(base_type=BQDataType.ARRAY, element_type=element_type)
            
            elif base_type == BQDataType.STRUCT:
                # Parse struct fields
                fields = TypeParser._parse_struct_fields(inner_str)
                return BigQueryType(base_type=BQDataType.STRUCT, fields=fields)
            
            elif base_type == BQDataType.RANGE:
                # Parse range type
                range_type = TypeParser.parse(inner_str)
                return BigQueryType(base_type=BQDataType.RANGE, range_type=range_type)
        
        # Handle parameterized types with ( )
        if '(' in type_str and type_str.endswith(')'):
            base_type_str = type_str[:type_str.index('(')]
            param_str = type_str[type_str.index('(')+1:-1]
            
            base_type = TypeParser._get_base_type(base_type_str)
            parameters = TypeParser._parse_parameters(param_str)
            
            return BigQueryType(base_type=base_type, parameters=parameters)
        
        # Simple type
        base_type = TypeParser._get_base_type(type_str)
        return BigQueryType(base_type=base_type)
    
    @staticmethod
    def _get_base_type(type_str: str) -> BQDataType:
        """Get base type from string, handling aliases."""
        type_str = type_str.strip().upper()
        
        # Check aliases first
        if type_str in TYPE_ALIASES:
            return TYPE_ALIASES[type_str]
        
        # Check direct type
        try:
            return BQDataType(type_str)
        except ValueError:
            raise ValueError(f"Unknown BigQuery type: {type_str}")
    
    @staticmethod
    def _parse_parameters(param_str: str) -> List[TypeParameter]:
        """Parse type parameters like '10' or '10, 2'."""
        parameters = []
        parts = param_str.split(',')
        
        for i, part in enumerate(parts):
            part = part.strip()
            if part.isdigit():
                if i == 0:
                    parameters.append(TypeParameter(name="precision", value=int(part)))
                else:
                    parameters.append(TypeParameter(name="scale", value=int(part)))
            else:
                parameters.append(TypeParameter(name=f"param{i}", value=part))
        
        return parameters
    
    @staticmethod
    def _parse_struct_fields(fields_str: str) -> List[Tuple[Optional[str], BigQueryType]]:
        """Parse struct field definitions."""
        fields = []
        
        # Simple parsing - doesn't handle nested complex types well
        # In production, this would need a proper parser
        parts = fields_str.split(',')
        
        for part in parts:
            part = part.strip()
            
            # Check if field has a name
            tokens = part.split(None, 1)
            if len(tokens) == 2 and not tokens[0].upper() in [t.value for t in BQDataType]:
                # Named field
                name = tokens[0]
                type_str = tokens[1]
                fields.append((name, TypeParser.parse(type_str)))
            else:
                # Anonymous field
                fields.append((None, TypeParser.parse(part)))
        
        return fields


class TypeValidator:
    """Validator for BigQuery type constraints."""
    
    @staticmethod
    def validate_parameterized_type(bq_type: BigQueryType) -> bool:
        """Validate parameterized type constraints."""
        if not bq_type.parameters:
            return True
        
        if bq_type.base_type == BQDataType.STRING:
            # STRING(L) - L must be positive INT64
            if len(bq_type.parameters) != 1:
                return False
            length = bq_type.parameters[0].value
            return isinstance(length, int) and length > 0
        
        elif bq_type.base_type == BQDataType.BYTES:
            # BYTES(L) - L must be positive INT64
            if len(bq_type.parameters) != 1:
                return False
            length = bq_type.parameters[0].value
            return isinstance(length, int) and length > 0
        
        elif bq_type.base_type in (BQDataType.NUMERIC, BQDataType.BIGNUMERIC):
            # NUMERIC(P, S) or NUMERIC(P)
            if len(bq_type.parameters) > 2:
                return False
            
            precision = bq_type.parameters[0].value
            scale = bq_type.parameters[1].value if len(bq_type.parameters) > 1 else 0
            
            if not isinstance(precision, int) or not isinstance(scale, int):
                return False
            
            if bq_type.base_type == BQDataType.NUMERIC:
                # NUMERIC: 0 ≤ S ≤ 9, max(1,S) ≤ P ≤ S + 29
                return (0 <= scale <= 9 and 
                        max(1, scale) <= precision <= scale + 29)
            else:
                # BIGNUMERIC: 0 ≤ S ≤ 38, max(1,S) ≤ P ≤ S + 38
                return (0 <= scale <= 38 and 
                        max(1, scale) <= precision <= scale + 38)
        
        return True
    
    @staticmethod
    def is_orderable(bq_type: BigQueryType) -> bool:
        """Check if type can be used in ORDER BY."""
        return bq_type.base_type in TypeProperties.ORDERABLE_TYPES
    
    @staticmethod
    def is_groupable(bq_type: BigQueryType) -> bool:
        """Check if type can be used in GROUP BY."""
        return bq_type.base_type in TypeProperties.GROUPABLE_TYPES
    
    @staticmethod
    def is_comparable(bq_type: BigQueryType) -> bool:
        """Check if type supports comparison operators."""
        return bq_type.base_type in TypeProperties.COMPARABLE_TYPES
    
    @staticmethod
    def is_collatable(bq_type: BigQueryType) -> bool:
        """Check if type supports collation."""
        if bq_type.base_type == BQDataType.STRING:
            return True
        elif bq_type.base_type == BQDataType.STRUCT:
            # Struct is collatable if it has string fields
            return any(field_type.base_type == BQDataType.STRING 
                      for _, field_type in bq_type.fields)
        elif bq_type.base_type == BQDataType.ARRAY:
            # Array is collatable if elements are strings
            return (bq_type.element_type and 
                    bq_type.element_type.base_type == BQDataType.STRING)
        return False


class TypeCaster:
    """Utilities for type casting and coercion."""
    
    # Implicit cast rules (from -> to)
    IMPLICIT_CASTS = {
        BQDataType.INT64: {BQDataType.FLOAT64, BQDataType.NUMERIC, BQDataType.BIGNUMERIC},
        BQDataType.FLOAT64: {BQDataType.NUMERIC, BQDataType.BIGNUMERIC},
        BQDataType.NUMERIC: {BQDataType.BIGNUMERIC},
    }
    
    @staticmethod
    def can_implicit_cast(from_type: BQDataType, to_type: BQDataType) -> bool:
        """Check if implicit cast is allowed."""
        if from_type == to_type:
            return True
        return to_type in TypeCaster.IMPLICIT_CASTS.get(from_type, set())
    
    @staticmethod
    def find_common_supertype(types: List[BQDataType]) -> Optional[BQDataType]:
        """Find common supertype for a list of types."""
        if not types:
            return None
        
        if len(types) == 1:
            return types[0]
        
        # All same type
        if all(t == types[0] for t in types):
            return types[0]
        
        # Check numeric types
        numeric_types = {BQDataType.INT64, BQDataType.FLOAT64, 
                        BQDataType.NUMERIC, BQDataType.BIGNUMERIC}
        
        if all(t in numeric_types for t in types):
            # Find highest precision numeric type
            if BQDataType.BIGNUMERIC in types:
                return BQDataType.BIGNUMERIC
            elif BQDataType.NUMERIC in types:
                return BQDataType.NUMERIC
            elif BQDataType.FLOAT64 in types:
                return BQDataType.FLOAT64
            else:
                return BQDataType.INT64
        
        # No common supertype
        return None


# Example usage functions
def parse_type_from_markdown(type_str: str) -> BigQueryType:
    """Parse a type string from the markdown documentation."""
    return TypeParser.parse(type_str)


def validate_type(bq_type: BigQueryType) -> Dict[str, bool]:
    """Validate type and return its properties."""
    return {
        "valid_parameters": TypeValidator.validate_parameterized_type(bq_type),
        "orderable": TypeValidator.is_orderable(bq_type),
        "groupable": TypeValidator.is_groupable(bq_type),
        "comparable": TypeValidator.is_comparable(bq_type),
        "collatable": TypeValidator.is_collatable(bq_type),
    }


def get_type_size(bq_type: BigQueryType) -> Optional[int]:
    """Get the fixed size of a type in logical bytes, if applicable."""
    size_info = TypeSize.get_size(bq_type.base_type)
    return size_info.fixed_size


if __name__ == "__main__":
    # Test the type parser
    test_types = [
        "INT64",
        "STRING(100)",
        "NUMERIC(10, 2)",
        "ARRAY<INT64>",
        "STRUCT<x INT64, y STRING(10)>",
        "RANGE<DATE>",
        "DECIMAL",  # Alias for NUMERIC
    ]
    
    print("=== BigQuery Type Parser Test ===\n")
    
    for type_str in test_types:
        try:
            bq_type = parse_type_from_markdown(type_str)
            print(f"Input: {type_str}")
            print(f"Parsed: {bq_type}")
            
            validation = validate_type(bq_type)
            print(f"Properties: {validation}")
            
            size = get_type_size(bq_type)
            if size:
                print(f"Fixed size: {size} bytes")
            
            print("-" * 40)
        except Exception as e:
            print(f"Error parsing '{type_str}': {e}")
            print("-" * 40)
    
    print("\n✅ BigQuery type support added! 🐕")