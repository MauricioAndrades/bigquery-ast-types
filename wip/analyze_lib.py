#!/usr/bin/env python3
"""
BigQuery AST Types Library Analyzer
Analyzes all Python files in the lib/ directory and generates a comprehensive report.
"""

import ast
import os
from pathlib import Path
from typing import Dict, List, Any
import json

class LibraryAnalyzer:
    def __init__(self, lib_path: str = './lib'):
        self.lib_path = Path(lib_path)
        self.results = {}
    
    def get_docstring(self, node) -> str:
        """Extract docstring from a node."""
        if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
            if node.body and isinstance(node.body[0], ast.Expr):
                if isinstance(node.body[0].value, (ast.Str, ast.Constant)):
                    value = node.body[0].value
                    return value.s if isinstance(value, ast.Str) else value.value
        return ""
    
    def analyze_file(self, filepath: Path) -> Dict[str, Any]:
        """Analyze a single Python file."""
        try:
            with open(filepath, 'r') as f:
                content = f.read()
                tree = ast.parse(content)
                
                result = {
                    'filename': filepath.name,
                    'lines': len(content.splitlines()),
                    'module_doc': self.get_docstring(tree),
                    'imports': [],
                    'classes': [],
                    'functions': [],
                    'constants': []
                }
                
                # Analyze imports
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            result['imports'].append(alias.name)
                    elif isinstance(node, ast.ImportFrom):
                        module = node.module or ''
                        for alias in node.names:
                            result['imports'].append(f"{module}.{alias.name}")
                
                # Analyze top-level definitions
                for node in tree.body:
                    if isinstance(node, ast.ClassDef):
                        class_info = self.analyze_class(node)
                        result['classes'].append(class_info)
                    
                    elif isinstance(node, ast.FunctionDef):
                        func_info = self.analyze_function(node)
                        result['functions'].append(func_info)
                    
                    elif isinstance(node, ast.Assign):
                        for target in node.targets:
                            if isinstance(target, ast.Name) and target.id.isupper():
                                result['constants'].append(target.id)
                
                return result
                
        except Exception as e:
            return {
                'filename': filepath.name,
                'error': str(e)
            }
    
    def analyze_class(self, node: ast.ClassDef) -> Dict[str, Any]:
        """Analyze a class definition."""
        class_info = {
            'name': node.name,
            'docstring': self.get_docstring(node),
            'bases': [],
            'decorators': [],
            'methods': [],
            'properties': [],
            'class_variables': []
        }
        
        # Get base classes
        for base in node.bases:
            if isinstance(base, ast.Name):
                class_info['bases'].append(base.id)
            else:
                try:
                    class_info['bases'].append(ast.unparse(base))
                except:
                    class_info['bases'].append(str(base))
        
        # Get decorators
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                class_info['decorators'].append(decorator.id)
            else:
                try:
                    class_info['decorators'].append(ast.unparse(decorator))
                except:
                    class_info['decorators'].append(str(decorator))
        
        # Analyze class body
        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                method_info = {
                    'name': item.name,
                    'params': [arg.arg for arg in item.args.args],
                    'decorators': [],
                    'is_static': False,
                    'is_class': False,
                    'is_property': False,
                    'is_private': item.name.startswith('_'),
                    'is_dunder': item.name.startswith('__') and item.name.endswith('__'),
                    'has_docstring': bool(self.get_docstring(item))
                }
                
                for decorator in item.decorator_list:
                    if isinstance(decorator, ast.Name):
                        decorator_name = decorator.id
                        method_info['decorators'].append(decorator_name)
                        if decorator_name == 'property':
                            method_info['is_property'] = True
                        elif decorator_name == 'staticmethod':
                            method_info['is_static'] = True
                        elif decorator_name == 'classmethod':
                            method_info['is_class'] = True
                
                if method_info['is_property']:
                    class_info['properties'].append(method_info)
                else:
                    class_info['methods'].append(method_info)
            
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                class_info['class_variables'].append(item.target.id)
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        class_info['class_variables'].append(target.id)
        
        return class_info
    
    def analyze_function(self, node: ast.FunctionDef) -> Dict[str, Any]:
        """Analyze a function definition."""
        return {
            'name': node.name,
            'params': [arg.arg for arg in node.args.args],
            'decorators': [
                d.id if isinstance(d, ast.Name) else str(d)
                for d in node.decorator_list
            ],
            'has_docstring': bool(self.get_docstring(node)),
            'is_async': isinstance(node, ast.AsyncFunctionDef)
        }
    
    def analyze_all(self):
        """Analyze all Python files in the library."""
        py_files = sorted(self.lib_path.glob('*.py'))
        
        for py_file in py_files:
            self.results[py_file.name] = self.analyze_file(py_file)
    
    def generate_report(self) -> str:
        """Generate a formatted report."""
        report = []
        report.append("=" * 80)
        report.append("BIGQUERY AST TYPES LIBRARY ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary statistics
        total_classes = sum(len(r.get('classes', [])) for r in self.results.values())
        total_functions = sum(len(r.get('functions', [])) for r in self.results.values())
        total_lines = sum(r.get('lines', 0) for r in self.results.values() if 'error' not in r)
        
        report.append("SUMMARY")
        report.append("-" * 40)
        report.append(f"Total Files: {len(self.results)}")
        report.append(f"Total Lines: {total_lines:,}")
        report.append(f"Total Classes: {total_classes}")
        report.append(f"Total Functions: {total_functions}")
        report.append("")
        
        # Detailed file analysis
        for filename in sorted(self.results.keys()):
            data = self.results[filename]
            
            report.append("=" * 80)
            report.append(f"FILE: {filename}")
            report.append("=" * 80)
            
            if 'error' in data:
                report.append(f"ERROR: {data['error']}")
                report.append("")
                continue
            
            if data.get('module_doc'):
                doc_lines = data['module_doc'].split('\n')
                report.append("MODULE DOCSTRING:")
                for line in doc_lines[:3]:
                    report.append(f"  {line}")
                if len(doc_lines) > 3:
                    report.append("  ...")
                report.append("")
            
            report.append(f"Lines of Code: {data.get('lines', 0)}")
            report.append(f"Imports: {len(data.get('imports', []))}")
            report.append("")
            
            # Classes
            classes = data.get('classes', [])
            if classes:
                report.append(f"CLASSES ({len(classes)}):")
                report.append("-" * 40)
                for cls in classes:
                    bases = f"({', '.join(cls['bases'])})" if cls['bases'] else ""
                    report.append(f"\n  class {cls['name']}{bases}:")
                    
                    if cls.get('decorators'):
                        report.append(f"    Decorators: {', '.join(cls['decorators'])}")
                    
                    if cls.get('docstring'):
                        doc_first_line = cls['docstring'].split('\n')[0]
                        report.append(f"    Doc: {doc_first_line[:60]}...")
                    
                    if cls.get('class_variables'):
                        report.append(f"    Class Variables: {', '.join(cls['class_variables'][:5])}")
                    
                    # Methods
                    methods = cls.get('methods', [])
                    if methods:
                        report.append(f"    Methods ({len(methods)}):")
                        
                        # Categorize methods
                        init = [m for m in methods if m['name'] == '__init__']
                        dunders = [m for m in methods if m['is_dunder'] and m['name'] != '__init__']
                        private = [m for m in methods if m['is_private'] and not m['is_dunder']]
                        public = [m for m in methods if not m['is_private'] and not m['is_dunder']]
                        
                        if init:
                            m = init[0]
                            params = ', '.join(m['params'][1:])  # Skip 'self'
                            report.append(f"      - __init__({params})")
                        
                        if dunders:
                            report.append(f"      Dunder methods: {', '.join(m['name'] for m in dunders)}")
                        
                        for m in public[:5]:
                            params = ', '.join(m['params'][1:]) if m['params'] else ''
                            decorators = f" [{', '.join(m['decorators'])}]" if m['decorators'] else ""
                            report.append(f"      - {m['name']}({params}){decorators}")
                        
                        if len(public) > 5:
                            report.append(f"      ... and {len(public)-5} more public methods")
                        
                        if private:
                            report.append(f"      Private methods: {', '.join(m['name'] for m in private[:3])}")
                            if len(private) > 3:
                                report.append(f"      ... and {len(private)-3} more private methods")
                    
                    # Properties
                    properties = cls.get('properties', [])
                    if properties:
                        report.append(f"    Properties ({len(properties)}):")
                        for prop in properties[:5]:
                            report.append(f"      - {prop['name']}")
                        if len(properties) > 5:
                            report.append(f"      ... and {len(properties)-5} more properties")
                
                report.append("")
            
            # Functions
            functions = data.get('functions', [])
            if functions:
                report.append(f"FUNCTIONS ({len(functions)}):")
                report.append("-" * 40)
                for func in functions:
                    params = ', '.join(func['params'])
                    decorators = f" [{', '.join(func['decorators'])}]" if func['decorators'] else ""
                    async_str = "async " if func.get('is_async') else ""
                    doc_str = " [documented]" if func.get('has_docstring') else ""
                    report.append(f"  - {async_str}{func['name']}({params}){decorators}{doc_str}")
                report.append("")
            
            # Constants
            constants = data.get('constants', [])
            if constants:
                report.append(f"CONSTANTS ({len(constants)}):")
                report.append("-" * 40)
                for const in constants[:10]:
                    report.append(f"  - {const}")
                if len(constants) > 10:
                    report.append(f"  ... and {len(constants)-10} more constants")
                report.append("")
        
        return '\n'.join(report)
    
    def save_json(self, filepath: str = 'lib_analysis.json'):
        """Save results as JSON."""
        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
    
    def save_report(self, filepath: str = 'lib_analysis_report.txt'):
        """Save the report to a file."""
        report = self.generate_report()
        with open(filepath, 'w') as f:
            f.write(report)


def main():
    """Main entry point."""
    # Use the lib directory in the current location
    lib_path = Path(__file__).parent / 'lib'
    if not lib_path.exists():
        print(f"Error: lib directory not found at {lib_path}")
        return
    
    analyzer = LibraryAnalyzer(lib_path)
    
    print(f"Analyzing library files in: {lib_path}")
    analyzer.analyze_all()
    
    print("\nGenerating report...")
    report = analyzer.generate_report()
    
    # Print to console
    print(report)
    
    # Save outputs
    analyzer.save_json('lib_analysis.json')
    analyzer.save_report('lib_analysis_report.txt')
    
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("  - Report saved to: lib_analysis_report.txt")
    print("  - JSON data saved to: lib_analysis.json")
    print("  - Commands log saved to: analysis_commands.log")


if __name__ == '__main__':
    main()