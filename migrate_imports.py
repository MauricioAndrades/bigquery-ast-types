#!/usr/bin/env python3
"""
Migration script to update imports after renaming bq_* files

This updates all imports from:
- bq_builders -> builders
- bq_ast_types -> ast_types  
- bq_parser -> parser

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import os
import glob


def update_imports_in_file(filepath):
    """Update imports in a single file."""
    
    replacements = [
        # Import statements
        ('from ast.bq_builders', 'from ast.builders'),
        ('from .bq_builders', 'from .builders'),
        ('import bq_builders', 'import builders'),
        
        ('from ast.bq_ast_types', 'from ast.ast_types'),
        ('from .bq_ast_types', 'from .ast_types'),
        ('import bq_ast_types', 'import ast_types'),
        
        ('from ast.bq_parser', 'from ast.parser'),
        ('from .bq_parser', 'from .parser'),
        ('import bq_parser', 'import parser'),
        
        # Also update references in __init__.py
        ('bq_parser import', 'parser import'),
        ('bq_builders import', 'builders import'),
        ('bq_ast_types import', 'ast_types import'),
    ]
    
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        
        original_content = content
        for old, new in replacements:
            content = content.replace(old, new)
        
        if content != original_content:
            with open(filepath, 'w') as f:
                f.write(content)
            print(f"Updated: {filepath}")
            return True
    except Exception as e:
        print(f"Error updating {filepath}: {e}")
        return False
    
    return False


def main():
    """Run migration on all Python files."""
    
    # Find all Python files
    python_files = []
    for root, dirs, files in os.walk('.'):
        # Skip sqlglot directory
        if 'sqlglot' in root:
            continue
        
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    
    print(f"Found {len(python_files)} Python files to check...")
    
    updated_count = 0
    for filepath in python_files:
        if update_imports_in_file(filepath):
            updated_count += 1
    
    print(f"\nMigration complete! Updated {updated_count} files.")
    
    # Also update any references in markdown files
    md_files = glob.glob('**/*.md', recursive=True)
    for md_file in md_files:
        if 'sqlglot' not in md_file:
            update_imports_in_file(md_file)


if __name__ == "__main__":
    main()
