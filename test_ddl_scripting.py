#!/usr/bin/env python3
"""
Test script to demonstrate DDL and scripting functionality.
This shows the new constructs added in Tasks 1 and 3.
"""

from lib.builders import Builders as b
from lib.serializer import SQLSerializer
from lib.types import Select, SelectColumn, Identifier, TableName, TableRef

def test_ddl_constructs():
    """Test DDL statement construction and serialization."""
    print("=== DDL Constructs ===")
    
    # Create a simple SELECT statement for testing
    select_stmt = Select(
        select_list=[SelectColumn(expression=Identifier('*'))],
        from_clause=TableRef(table=TableName(table='users'))
    )
    
    # CREATE VIEW
    view = b.create_view('user_view', select_stmt, or_replace=True, materialized=True)
    print("CREATE VIEW:", view)
    
    # CREATE FUNCTION
    func = b.create_function('double_it', [('x', 'INT64')], 'INT64', body='x * 2')
    print("CREATE FUNCTION:", func)
    
    # ALTER TABLE with multiple actions
    alter_stmt = b.alter_table(
        'users',
        b.add_column('email', 'STRING', not_null=True),
        b.rename_column('old_name', 'new_name'),
        b.drop_column('deprecated_col', if_exists=True)
    )
    print("ALTER TABLE:", alter_stmt)
    
    # DROP statement
    drop_stmt = b.drop('TABLE', 'old_table', if_exists=True, cascade=True)
    print("DROP:", drop_stmt)
    
    return [view, func, alter_stmt, drop_stmt]

def test_scripting_constructs():
    """Test scripting statement construction and serialization."""
    print("\n=== Scripting Constructs ===")
    
    # DECLARE variables
    declare_stmt = b.declare(
        ('counter', 'INT64', b.lit(0)),
        ('name', 'STRING', None)
    )
    print("DECLARE:", declare_stmt)
    
    # SET variable
    set_stmt = b.set_var('counter', b.lit(10))
    print("SET:", set_stmt)
    
    # IF statement
    if_stmt = b.if_(
        b.gt(b.col('counter'), b.lit(5)),
        b.set_var('result', b.lit('high'))
    )
    print("IF:", if_stmt)
    
    # WHILE loop
    while_stmt = b.while_(
        b.gt(b.col('counter'), b.lit(0)),
        b.set_var('counter', b.lit(5)),  # Simplified without subtraction
        label='countdown'
    )
    print("WHILE:", while_stmt)
    
    # FOR loop
    select_stmt = Select(
        select_list=[SelectColumn(expression=Identifier('id'))],
        from_clause=TableRef(table=TableName(table='items'))
    )
    for_stmt = b.for_(
        'item',
        select_stmt,
        b.set_var('total', b.lit(100))  # Simplified without addition
    )
    print("FOR:", for_stmt)
    
    # BEGIN-END block
    begin_stmt = b.begin_end(
        b.set_var('x', b.lit(1)),
        b.set_var('y', b.lit(2)),
        label='calculation'
    )
    print("BEGIN-END:", begin_stmt)
    
    # Control flow
    break_stmt = b.break_('loop_label')
    continue_stmt = b.continue_()
    call_stmt = b.call('my_procedure', b.lit('arg1'), b.lit(42))
    
    print("BREAK:", break_stmt)
    print("CONTINUE:", continue_stmt)
    print("CALL:", call_stmt)
    
    return [declare_stmt, set_stmt, if_stmt, while_stmt, for_stmt, begin_stmt, break_stmt, continue_stmt, call_stmt]

def test_serialization():
    """Test SQL serialization of new constructs."""
    print("\n=== Serialization ===")
    
    serializer = SQLSerializer()
    
    # Test DDL
    ddl_constructs = test_ddl_constructs()
    for i, construct in enumerate(ddl_constructs):
        try:
            sql = serializer.serialize(construct)
            print(f"DDL {i+1}: {sql}")
        except Exception as e:
            print(f"DDL {i+1} error: {e}")
    
    # Test Scripting
    script_constructs = test_scripting_constructs()
    for i, construct in enumerate(script_constructs):
        try:
            sql = serializer.serialize(construct)
            print(f"Script {i+1}: {sql}")
        except Exception as e:
            print(f"Script {i+1} error: {e}")

if __name__ == '__main__':
    test_ddl_constructs()
    test_scripting_constructs()
    test_serialization()
    print("\n=== All DDL and Scripting Tests Completed ===")