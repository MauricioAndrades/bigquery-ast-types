# Migrating to AST-Based SQL Transformations

## Why We're Switching to AST-Based SQL Transformations

As our retail analytics platform has grown, supporting dozens of retailers with unique data quirks, we reached the limits of traditional, string-based SQL templating. Previously, our query generation relied on f-strings, regexes, or manual copy-paste—resulting in:

- **Code Duplication:** 50+ lines of retailer-specific SQL logic per retailer.
- **Schema Drift:** Inconsistent column names, types, and missing fields.
- **Bug Risk:** Brittle transformations that broke with query changes.
- **Poor Maintainability:** Tedious updates whenever schemas changed.

### The Solution: AST-Based, Standardized SQL

We are migrating to an architecture where **all SQL transformations occur via Abstract Syntax Tree (AST) traversal and mutation**. This means:

- **Standardization:** Every retailer’s logic is merged into a common 3-CTE pipeline (`RAW_ORDERS`, `CLEANED_ORDERS`, `DEDUPED_ORDERS`) with enforced schema compliance and deduplication.
- **Flexibility:** Retailer-specific quirks are injected as column overrides or config, not as whole-query rewrites.
- **Safety:** All SQL is parsed, mutated, and validated as an AST before being serialized—no more brittle string replacements.
- **Maintainability:** Adding a new retailer or fixing a schema mapping is as simple as editing a config or override, not hand-writing SQL.

**In short: We get the best of both worlds—global consistency and local flexibility.**

---

## Instructions: How to Fix/Update Existing Retailer Pipelines

### 1. **Stop Using String-Based SQL Templates**
- Do **not** write new SQL with f-strings, `.format()`, or regex replacements.
- All new query business logic must be written as raw SQL, then parsed and transformed using our AST pipeline.

### 2. **Adopt the 3-CTE Pipeline Standard**

Every retailer pipeline now must fit this structure:
- **CTE 1 `RAW_ORDERS`:** Raw business logic, as close to the retailer’s data as possible.
- **CTE 2 `CLEANED_ORDERS`:** Apply standardization (casting, null handling, renaming, etc).
- **CTE 3 `DEDUPED_ORDERS`:** Guaranteed deduplication and schema compliance.

### 3. **Specify Column Overrides in Config**

Instead of custom SQL, use the `overrides` config to define retailer-specific quirks:
```yaml
overrides:
  visitor_id: "CASE WHEN visitor_id IN ('nico-nico') THEN NULL ELSE visitor_id END"
  platform: "CASE WHEN platform IN ('ios','android') THEN 2 ELSE 1 END"
```
- Overrides are applied in the CLEANED_ORDERS CTE via AST mutation.

### 4. **Rely on the AST Transformation Engine**

- The engine will:
    - Parse your `RAW_ORDERS` SQL.
    - Apply schema-based casting, null handling, and missing columns.
    - Inject your overrides at the right stage.
    - Always enforce deduplication and schema compliance.

### 5. **Testing and Validation**

- Run the provided `validate_sql_ast.py` script to ensure your pipeline compiles and matches the expected schema.
- For complex cases, add unit tests on the output SQL or the transformed AST.

### 6. **How to Fix Existing Pipelines**

- **Step 1:** Move all business logic into the `RAW_ORDERS` CTE.
- **Step 2:** Remove all string-based hacks from your pipeline.
- **Step 3:** Extract retailer-specific quirks into the `overrides` config.
- **Step 4:** Validate with `validate_sql_ast.py`.

---

## Core Traversal Algorithm

The following pseudocode shows how our transformation engine processes and standardizes retailer SQL via AST traversal:

```python
def transform_sql_pipeline(
    raw_orders_sql: str, 
    target_schema: dict, 
    overrides: dict
) -> str:
    """
    Transform a retailer's raw SQL into a standardized, deduped pipeline using AST traversal.
    """
    # 1. Parse RAW_ORDERS into AST
    ast = parse_sql_to_ast(raw_orders_sql, dialect="bigquery")
    
    # 2. Traverse CLEANED_ORDERS
    for col in target_schema:
        if col in overrides:
            # Apply retailer-specific override
            ast = replace_column_expr(ast, col, overrides[col])
        else:
            # Apply standard type casting and null handling as per schema
            ast = enforce_column_schema(ast, col, target_schema[col])
    
    # 3. Ensure all required columns present
    ast = add_missing_columns(ast, target_schema)
    
    # 4. Inject DEDUPED_ORDERS logic
    ast = insert_deduplication_logic(ast, dedupe_keys=target_schema['dedupe_keys'])
    
    # 5. Validate AST
    validate_ast(ast, target_schema)
    
    # 6. Serialize back to SQL
    return serialize_ast_to_sql(ast, dialect="bigquery")
```

**In summary:**  
- We parse the original retailer SQL to AST,
- Traverse and mutate it to apply both standardization and retailer-specific overrides,
- Guarantee schema and deduplication,
- And output robust, maintainable SQL—all before runtime.

---

_For questions, see `CONTRIBUTING.md` or ask in Slack #data-engineering._
