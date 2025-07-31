"""
BigQuery AST Types Library

Core library modules for AST manipulation.
"""

# Re-export all public APIs from submodules
from .types import *
from .node_path import NodePath, create_path, get_node_at_path
from .visitor import BaseVisitor, visit
from .scope import Scope
from .serializer import (
    SQLSerializer,
    SerializerOptions,
    to_sql,
    pretty_print,
    compact_print
)

# Import these separately to avoid circular imports
try:
    from .builders import b, Builders, ValidationError
except ImportError:
    pass

try:
    from .collection import Collection
except ImportError:
    pass