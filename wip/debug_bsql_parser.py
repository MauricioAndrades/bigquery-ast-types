#!/usr/bin/env python3
"""
Debug version of the bsql parser test with multiple debugging approaches
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from lib.bsql import j, Iterator, SQLNode, extract_table_references
from sqlglot import exp
import pdb  # Python debugger
try:
    import ipdb  # Enhanced debugger (if installed)
except ImportError:
    ipdb = None

# Shorter query for debugging
SIMPLE_QUERY = """
WITH orders AS (
  SELECT 
    order_id,
    COUNT(*) as order_count
  FROM `symbiosys-prod.event_api.order`
  WHERE retailer_id = 14
  GROUP BY order_id
)
SELECT * FROM orders
"""

def debug_with_pdb():
    """Debug using Python's built-in debugger"""
    print("\n" + "="*60)
    print("DEBUGGING WITH PDB")
    print("="*60)
    
    # Parse the query
    ast = j.parse(SIMPLE_QUERY)
    
    # Set a breakpoint here - the debugger will stop
    pdb.set_trace()
    
    # You can now inspect variables:
    # (Pdb) type(ast)
    # (Pdb) ast.node
    # (Pdb) dir(ast)
    # (Pdb) ast.node.sql(dialect='bigquery', pretty=True)
    
    # Find CTEs
    ctes = Iterator(ast.node, exp.CTE).toList()
    print(f"Found {len(ctes)} CTEs")
    
    return ast

def debug_with_print_statements():
    """Debug using strategic print statements"""
    print("\n" + "="*60)
    print("DEBUGGING WITH PRINT STATEMENTS")
    print("="*60)
    
    # Parse the query
    print(f"📍 Parsing query...")
    ast = j.parse(SIMPLE_QUERY)
    print(f"   Type of ast: {type(ast)}")
    print(f"   Type of ast.node: {type(ast.node)}")
    print(f"   AST class name: {ast.node.__class__.__name__}")
    
    # Inspect the AST structure
    print(f"\n📍 AST Node Attributes:")
    for attr in dir(ast.node):
        if not attr.startswith('_'):
            print(f"   - {attr}")
    
    # Look at args
    print(f"\n📍 AST args:")
    if hasattr(ast.node, 'args'):
        for key, value in ast.node.args.items():
            print(f"   {key}: {type(value).__name__}")
            if value:
                print(f"      -> {str(value)[:100]}...")
    
    # Walk the tree
    print(f"\n📍 Walking the AST tree:")
    for i, node in enumerate(ast.node.walk()):
        if i < 10:  # First 10 nodes
            print(f"   {i}. {type(node).__name__}: {str(node)[:50]}...")
    
    return ast

def debug_with_ipython():
    """Debug using IPython for interactive exploration"""
    print("\n" + "="*60)
    print("DEBUGGING WITH IPYTHON")
    print("="*60)
    
    try:
        from IPython import embed
        
        # Parse the query
        ast = j.parse(SIMPLE_QUERY)
        ctes = Iterator(ast.node, exp.CTE).toList()
        tables = extract_table_references(ast.node)
        
        print("Variables available for inspection:")
        print("  - ast: The parsed AST")
        print("  - ctes: List of CTEs")
        print("  - tables: List of table references")
        print("\nDropping into IPython shell...")
        print("Try: ast.node.sql(dialect='bigquery', pretty=True)")
        print("Try: ctes[0].node.alias")
        print("Try: ast.find(exp.Table).toList()")
        
        # This will drop you into an interactive IPython shell
        embed()
        
    except ImportError:
        print("IPython not installed. Install with: pip install ipython")
    
    return ast

def debug_specific_node():
    """Debug a specific type of node in detail"""
    print("\n" + "="*60)
    print("DEBUGGING SPECIFIC NODE TYPE (CTE)")
    print("="*60)
    
    ast = j.parse(SIMPLE_QUERY)
    
    # Find first CTE
    first_cte = Iterator(ast.node, exp.CTE).first()
    
    if first_cte:
        print(f"📍 Found CTE node: {type(first_cte)}")
        print(f"   SQLNode wrapper: {first_cte}")
        print(f"   Underlying node: {first_cte.node}")
        
        # Inspect CTE structure
        print(f"\n📍 CTE attributes:")
        cte_node = first_cte.node
        print(f"   - alias: {cte_node.alias}")
        print(f"   - this (query): {type(cte_node.this).__name__}")
        
        # Get the SELECT inside the CTE
        if isinstance(cte_node.this, exp.Select):
            select = cte_node.this
            print(f"\n📍 SELECT inside CTE:")
            print(f"   - expressions: {len(select.expressions)} columns")
            for i, expr in enumerate(select.expressions):
                print(f"     {i+1}. {expr}")
            
            # FROM clause
            if select.args.get('from'):
                from_table = select.args['from'].this
                print(f"   - FROM: {from_table}")
            
            # WHERE clause
            if select.args.get('where'):
                where = select.args['where']
                print(f"   - WHERE: {where.this}")
    
    return ast

def debug_iterator_operations():
    """Debug Iterator class operations"""
    print("\n" + "="*60)
    print("DEBUGGING ITERATOR OPERATIONS")
    print("="*60)
    
    ast = j.parse(SIMPLE_QUERY)
    
    # Create an iterator for all expressions
    print("📍 Creating Iterator for all Expressions")
    all_exprs = Iterator(ast.node, exp.Expression)
    
    # Count them
    count = 0
    for expr in all_exprs:
        count += 1
    print(f"   Total expressions: {count}")
    
    # Filter for specific types
    print("\n📍 Filtering for Column references")
    columns = Iterator(ast.node, exp.Column).toList()
    print(f"   Found {len(columns)} columns:")
    for col in columns:
        print(f"     - {col.node}")
    
    # Use map
    print("\n📍 Using map to get column names")
    column_names = Iterator(ast.node, exp.Column).map(
        lambda n: n.node.this if hasattr(n.node, 'this') else str(n.node)
    )
    print(f"   Column names: {column_names}")
    
    # Chain operations
    print("\n📍 Chaining filter operations")
    literals = Iterator(ast.node, exp.Literal).filter(
        lambda n: n.node.is_int if hasattr(n.node, 'is_int') else False
    ).toList()
    print(f"   Integer literals: {[l.node.this for l in literals]}")
    
    return ast

def debug_transformation():
    """Debug AST transformation"""
    print("\n" + "="*60)
    print("DEBUGGING TRANSFORMATION")
    print("="*60)
    
    ast = j.parse(SIMPLE_QUERY)
    
    print("📍 Original SQL:")
    print(ast.sql(dialect='bigquery', pretty=False)[:200] + "...")
    
    # Define transformation function
    def transform_func(node):
        # Debug: print what nodes we're visiting
        if isinstance(node, exp.Literal):
            print(f"   Visiting literal: {node.this}")
            if node.this == "14":
                print(f"     -> Replacing with parameter")
                return exp.Placeholder(this="@retailer_id")
        return node
    
    print("\n📍 Applying transformation:")
    transformed = ast.transform(transform_func)
    
    print("\n📍 Transformed SQL:")
    print(transformed.sql(dialect='bigquery', pretty=False)[:200] + "...")
    
    return transformed

def interactive_debugging_session():
    """Full interactive debugging session"""
    print("\n" + "="*60)
    print("INTERACTIVE DEBUGGING SESSION")
    print("="*60)
    print("""
Choose debugging method:
1. pdb.set_trace() - Built-in Python debugger
2. Print statements - Strategic printing
3. IPython shell - Interactive exploration
4. Specific node inspection
5. Iterator operations
6. Transformation debugging
7. Run all methods
    """)
    
    choice = input("Enter choice (1-7): ").strip()
    
    if choice == "1":
        debug_with_pdb()
    elif choice == "2":
        debug_with_print_statements()
    elif choice == "3":
        debug_with_ipython()
    elif choice == "4":
        debug_specific_node()
    elif choice == "5":
        debug_iterator_operations()
    elif choice == "6":
        debug_transformation()
    elif choice == "7":
        # Run all except interactive ones
        debug_with_print_statements()
        debug_specific_node()
        debug_iterator_operations()
        debug_transformation()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    # You can also just set a breakpoint here in your IDE
    # Most IDEs (VSCode, PyCharm) will stop here if you run in debug mode
    
    # For quick testing, uncomment one:
    # debug_with_pdb()  # Will stop at breakpoint
    # debug_with_print_statements()  # Will show detailed output
    # debug_specific_node()  # Inspect CTE structure
    # debug_iterator_operations()  # See how Iterator works
    # debug_transformation()  # Watch transformation happen
    
    # Or run interactive menu:
    interactive_debugging_session()