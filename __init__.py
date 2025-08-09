"""
BigQuery AST Types

A comprehensive AST library for BigQuery SQL manipulation.
"""

if __package__:
    # Core types
    from .lib.types import *

    # Node path and traversal
    from .lib.node_path import NodePath, create_path, get_node_at_path

    # Visitor pattern
    from .lib.visitor import BaseVisitor, visit

    # Builder API
    from .lib.builders import b, Builders, ValidationError

    # Scope management
    from .lib.scope import Scope

    # Serialization
    from .lib.serializer import (
        SQLSerializer,
        SerializerOptions,
        to_sql,
        pretty_print,
        compact_print
    )

    # Collection utilities
    from .lib.collection import Collection, create_collection
    
    # BSQL utilities
    from .lib.bsql import Iterator, SQLNode, j

    # Parsers
    from .parsers.sqlglot import parse, SQLGlotParser
else:
    # When imported without a package context, skip heavy imports.
    pass

# Version
__version__ = "0.1.0"

# Main exports
__all__ = [
    # Types
    "ASTNode",
    "Expression", 
    "Statement",
    "Identifier",
    "Literal",
    "StringLiteral",
    "IntegerLiteral", 
    "FloatLiteral",
    "BooleanLiteral",
    "NullLiteral",
    "BinaryOp",
    "UnaryOp",
    "FunctionCall",
    "Select",
    "SelectColumn",
    "TableRef",
    "TableName",
    "Join",
    "JoinType",
    "WhereClause",
    "Merge",
    "MergeAction",
    "MergeInsert",
    "MergeUpdate",
    "MergeDelete",
    "Case",
    "WhenClause",
    "Insert",
    "Update",
    "CreateTable",
    "Subquery",
    
    # Enums
    "JoinType",
    "ComparisonOp",
    "LogicalOp",
    "ArithmeticOp",
    "OrderDirection",
    
    # NodePath
    "NodePath",
    "create_path",
    "get_node_at_path",
    
    # Visitor
    "BaseVisitor",
    "visit",
    "ASTVisitor",
    
    # Builders
    "b",
    "Builders",
    "ValidationError",
    
    # Scope
    "Scope",
    
    # Serializer
    "SQLSerializer",
    "SerializerOptions", 
    "to_sql",
    "pretty_print",
    "compact_print",
    
    # Collection
    "Collection",
    "create_collection",
    
    # Parser
    "parse",
    "SQLGlotParser",
]