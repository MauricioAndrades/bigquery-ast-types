# Help Guide: Migrating Our Query Standardization to `sqlglot`

## Why We're Switching to `sqlglot`

As our data platform grows, we need both **standardization** and **flexibility** in how we generate and transform SQL across many retailers and business cases. Our previous approaches—using f-strings, manual string manipulation, or libraries like `sqlparse`—were not robust enough for BigQuery’s full SQL dialect and often led to bugs, schema drift, and hard-to-maintain code.

`sqlglot` is a Python library that supports AST parsing and transformation for BigQuery SQL. It enables us to:

- Parse real BigQuery queries into a true AST (not just tokens)
- Traverse, inspect, and mutate any part of a query (SELECT, CTE, WHERE, etc.)
- Apply both global standardizations and retailer-specific overrides reliably
- Output valid, well-formatted BigQuery SQL
- Future-proof our standardization approach against new query patterns (not just 3-CTE!)

---

## What Needs to Change

### 1. Review Previous Work

**Examples of old approaches:**
- f-string and `.format()`-style SQL templates
- String concatenation and regex replacements for overrides
- Using `sqlparse` for token-level edits (not AST-based)
- Hardcoding business logic into SQL pipeline files

**Problems with these approaches:**
- Brittle when queries change
- No way to validate output schema programmatically
- Difficult to extend or debug

### 2. What We Need to Do Now

- **Stop using string hacks** for SQL generation and overrides.
- **Replace any use of `sqlparse` with `sqlglot`.**
- **Move all retailer-specific "diffs" into structured config/override objects.**
- **Adopt a standard traversal/mutation pattern for query transformation.**
- **Test the output: queries should be valid BigQuery SQL and conform to target schemas.**

---

## Migration Steps

### Step 1: Install `sqlglot`

```bash
pip install sqlglot
```

### Step 2: Refactor Query Code to Use `sqlglot`

#### Old pattern (do not use):

```python
# BAD: f-string or regex replacement
source = f"SELECT {custom_expr} AS visitor_id FROM {table}"
clean = re.sub("visitor_id AS .*", override_expr, source)
```

#### New pattern (with `sqlglot`):

```python
import sqlglot

def apply_overrides(query: str, overrides: dict) -> str:
    ast = sqlglot.parse_one(query, read='bigquery')
    for col, expr in overrides.items():
        ast = ast.transform(
            lambda node: sqlglot.exp.alias_(sqlglot.parse_one(expr), col) if node.is_star or node.alias_or_name == col else node
        )
    return ast.sql(dialect='bigquery')

# Usage
base_query = "SELECT visitor_id, order_id FROM raw_orders"
overrides = {"visitor_id": "CASE WHEN visitor_id IN ('nico-nico') THEN NULL ELSE visitor_id END"}
new_query = apply_overrides(base_query, overrides)
print(new_query)
```

### Step 3: Move Overrides and Schema Rules to Config

Define overrides in YAML, JSON, or Python dicts (not in SQL pipelines):

```yaml
# overrides/gopuff.yaml
visitor_id: "CASE WHEN visitor_id IN ('nico-nico') THEN NULL ELSE visitor_id END"
platform: "CASE WHEN platform IN ('ios','android') THEN 2 ELSE 1 END"
```

### Step 4: Traverse All Query Patterns

- Don't assume a 3-CTE structure; support any number or shape of CTEs, subqueries, or select lists.
- Use `sqlglot`'s AST traversal to find and override columns, inject deduplication logic, or enforce schema as needed.

### Step 5: Validate and Test

- Use `sqlglot` to parse and re-emit the final SQL. If it fails, the query is invalid.
- Add unit tests for all major pipeline patterns, including before/after migration.

---

## Example: AST Traversal Algorithm

```python
def standardize_query(raw_sql: str, target_schema: dict, overrides: dict) -> str:
    ast = sqlglot.parse_one(raw_sql, read="bigquery")

    # 1. Apply column overrides
    for col, expr in overrides.items():
        ast = ast.transform(
            lambda node: sqlglot.exp.alias_(sqlglot.parse_one(expr), col) if node.alias_or_name == col else node
        )

    # 2. Ensure all target schema columns exist (add missing, type cast, etc.)
    for col, col_type in target_schema.items():
        if not ast.find(sqlglot.exp.Column, lambda n: n.alias_or_name == col):
            # Add missing columns as NULL or default
            ast.select(sqlglot.exp.alias_(sqlglot.parse_one(f"CAST(NULL AS {col_type})"), col))

    # 3. (Optional) Inject deduplication or other required logic

    return ast.sql(dialect="bigquery")
```

---

## What You Should Do

1. **Review** your pipeline and remove all string-based SQL hacks.
2. **Convert** your business logic to plain SQL, and load it via string or file.
3. **Define** all retailer or business-specific overrides in config.
4. **Use** the migration patterns above with `sqlglot` for all transformations.
5. **Test** your pipeline outputs—make sure they run in BigQuery and conform to the expected schema.

---

## Need Help?

If you’re stuck or want a code review on your migration, tag @MauricioAndrades or ask in #data-engineering.
