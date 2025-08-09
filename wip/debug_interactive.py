#!/usr/bin/env python3
"""
Interactive debugging session for bsql parser
Run this and it will drop you into a debugger
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from lib.bsql import j, Iterator, SQLNode
from sqlglot import exp
import pdb

# Query to debug
QUERY = """
SELECT 
  order_id,
  COUNT(*) as count
FROM `symbiosys-prod.event_api.order`
WHERE retailer_id = 14
"""

print("🔍 Starting Interactive Debug Session")
print("=" * 60)
print("Parsing query and dropping into debugger...")
print()
print("Useful commands in debugger:")
print("  n     - next line")
print("  s     - step into function")
print("  c     - continue execution")
print("  l     - list current code")
print("  p var - print variable")
print("  pp var - pretty print variable")
print("  h     - help")
print("  quit  - exit debugger")
print()
print("Try these in the debugger:")
print("  p ast")
print("  p type(ast.node)")
print("  p ast.node.sql(dialect='bigquery')")
print("  p list(ast.node.walk())[:5]")
print("=" * 60)
print()

# Parse the query
ast = j.parse(QUERY)

# THIS IS WHERE THE DEBUGGER STOPS
# You can inspect everything from here
pdb.set_trace()

# Find all tables
tables = Iterator(ast.node, exp.Table).toList()
print(f"Found {len(tables)} tables")

# Find columns
columns = Iterator(ast.node, exp.Column).toList()
print(f"Found {len(columns)} columns")

# Find WHERE clause
where = Iterator(ast.node, exp.Where).first()
if where:
    print(f"WHERE clause: {where.node}")

print("\n✅ Debug session complete!")