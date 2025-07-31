# BQAST CLI Tool Usage Guide

The `bq_transform.py` CLI tool transforms simple SQL queries into production-ready BigQuery MERGE statements using AST transformations.

## Installation

```bash
cd apps/composer/bqast
chmod +x bq_transform.py
```

## Basic Usage

### Transform Inline SQL

```bash
./bq_transform.py -r 12345 "SELECT * FROM orders WHERE date > '2024-01-01'"
```

### Transform from File

```bash
./bq_transform.py -f my_query.sql -r 12345 -p my-project -d analytics
```

### Analyze Query Only

```bash
./bq_transform.py --analyze -f complex_query.sql
```

### Run Demo

```bash
./bq_transform.py --demo
```

## Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `-r, --retailer ID` | Retailer ID for the transformation | Yes | - |
| `-f, --file FILE` | Read SQL from file instead of command line | No | - |
| `-p, --project PROJECT` | BigQuery project ID | No | `my-project` |
| `-d, --dataset DATASET` | BigQuery dataset name | No | `event_analytics` |
| `-t, --threshold DATE` | DateTime threshold (YYYY-MM-DD HH:MM:SS) | No | 30 days ago |
| `-o, --output FILE` | Write output to file instead of stdout | No | - |
| `--analyze` | Only analyze the query, don't transform | No | False |
| `--validate` | Validate the generated SQL | No | False |
| `--demo` | Run demo with example SQL | No | False |

## Examples

### 1. Simple Transformation

```bash
./bq_transform.py -r 99999 "SELECT order_id, product_id FROM orders"
```

This will:
- Analyze the query
- Transform it to a 3-CTE merge pattern
- Add standardization and deduplication
- Output the result to stdout

### 2. File Input with Custom Project

```bash
./bq_transform.py \
  -f queries/daily_orders.sql \
  -r 12345 \
  -p production-project \
  -d warehouse \
  -o output/daily_orders_merge.sql
```

### 3. Analysis Only

```bash
./bq_transform.py --analyze -f complex_join_query.sql
```

Output:
```
Query Analysis
==============
Tables: orders, customers, products
Columns: 15 unique
Aggregations: 3
Joins: 2
Subqueries: 1
Complexity Score: 8
```

### 4. With Validation

```bash
./bq_transform.py \
  -f query.sql \
  -r 12345 \
  --validate \
  -o validated_output.sql
```

This ensures the generated SQL is valid BigQuery syntax.

### 5. Retailer-Specific Transformations

```bash
# Chewy (ID: 1001)
./bq_transform.py -r 1001 -f chewy_orders.sql

# Best Buy (ID: 2001)  
./bq_transform.py -r 2001 -f bestbuy_orders.sql

# Target (ID: 3001)
./bq_transform.py -r 3001 -f target_orders.sql
```

## Output Format

The tool generates a complete MERGE statement with:

1. **WITH clause** containing:
   - `raw_orders` - Original query as CTE
   - `cleaned_orders` - Standardized columns
   - `deduped_orders` - Deduplicated rows

2. **MERGE statement** with:
   - Null-safe join conditions
   - INSERT for new records
   - DELETE for orphaned records

## Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| "Retailer ID is required" | Missing `-r` flag | Provide retailer ID |
| "Failed to read file" | File not found | Check file path |
| "Analysis failed" | Invalid SQL | Check SQL syntax |
| "Transformation failed" | Unsupported SQL features | Simplify query |
| "Validation failed" | Generated SQL is invalid | Report bug |

## Tips

1. **Start with Analysis**: Use `--analyze` first to understand your query complexity
2. **Use Demo Mode**: Run `--demo` to see example transformations
3. **Validate Complex Queries**: Always use `--validate` for complex transformations
4. **Save Outputs**: Use `-o` to save generated SQL for review
5. **Check Thresholds**: Ensure datetime thresholds match your data retention

## Integration with Airflow

```python
from airflow.operators.bash import BashOperator

transform_task = BashOperator(
    task_id='transform_order_sql',
    bash_command='''
        cd /path/to/bqast && \
        ./bq_transform.py \
            -f {{ dag_run.conf["sql_file"] }} \
            -r {{ dag_run.conf["retailer_id"] }} \
            -t "{{ ds }} 00:00:00" \
            -o /tmp/transformed_{{ ds }}.sql
    ''',
)
```

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`:
```bash
cd apps/composer
source venv/bin/activate
pip install sqlglot
```

### Permission Errors

```bash
chmod +x bq_transform.py
```

### Large Queries

For queries over 1000 lines, use file input:
```bash
./bq_transform.py -f large_query.sql -r 12345
```

---

Built with ❤️ by Little Bow Wow 🐕
