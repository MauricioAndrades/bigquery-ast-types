# BigQuery ML and External Table Support Implementation

## Overview
This implementation adds comprehensive BigQuery ML and External Table support to the AST types and builders, following the established patterns in the repository.

## New AST Nodes Added

### BigQuery ML Nodes

#### CreateModel(Statement)
- **Fields**: `model_name: str`, `options: Dict[str, Expression]`, `as_query: Optional[Select]`, `transform: Optional[Expression]`
- **Purpose**: Represents CREATE MODEL statements for BigQuery ML
- **Example**: `CREATE MODEL dataset.model OPTIONS(model_type='linear_reg') AS SELECT ...`

#### MLPredict(Expression)
- **Fields**: `model_name: str`, `input_data: Union[Select, TableRef]`, `struct_options: Dict[str, Expression]`
- **Purpose**: Represents ML.PREDICT function calls
- **Example**: `ML.PREDICT(MODEL dataset.model, table_name, STRUCT(0.5 AS threshold))`

#### MLEvaluate(Expression)
- **Fields**: `model_name: str`, `input_data: Union[Select, TableRef]`, `struct_options: Dict[str, Expression]`
- **Purpose**: Represents ML.EVALUATE function calls
- **Example**: `ML.EVALUATE(MODEL dataset.model, test_data)`

#### MLExplain(Expression)
- **Fields**: `model_name: str`, `input_data: Union[Select, TableRef]`, `struct_options: Dict[str, Expression]`
- **Purpose**: Represents ML.EXPLAIN_PREDICT function calls
- **Example**: `ML.EXPLAIN_PREDICT(MODEL dataset.model, input_data)`

### External Table Nodes

#### CreateExternalTable(Statement)
- **Fields**: `table_name: str`, `schema: List[str]`, `options: Dict[str, Expression]`
- **Purpose**: Represents CREATE EXTERNAL TABLE statements
- **Example**: `CREATE EXTERNAL TABLE dataset.table (id INT64) OPTIONS(format='CSV', uris=['gs://bucket/file.csv'])`

#### ExportData(Statement)
- **Fields**: `as_query: Select`, `options: Dict[str, Expression]`
- **Purpose**: Represents EXPORT DATA statements
- **Example**: `EXPORT DATA OPTIONS(uri='gs://bucket/export.csv') AS SELECT * FROM table`

#### LoadData(Statement)
- **Fields**: `table_name: str`, `source_uris: List[str]`, `options: Dict[str, Expression]`
- **Purpose**: Represents LOAD DATA statements
- **Example**: `LOAD DATA INTO table FROM FILES('gs://bucket/data.csv')`

## New Builder Methods

### BigQuery ML Builders

#### `b.create_model(model_name, options=None, as_query=None, transform=None)`
- **Validation**: Non-empty model name, valid options dictionary
- **Returns**: `CreateModel` AST node
- **Features**: Converts options to appropriate literal types, handles list values

#### `b.ml_predict(model_name, input_data, struct_options=None)`
- **Validation**: Non-empty model name, input_data must be Select or TableRef
- **Returns**: `MLPredict` AST node
- **Features**: Handles both table references and SELECT queries as input

### External Table Builders

#### `b.create_external_table(table_name, schema=None, options=None)`
- **Validation**: Non-empty table name, schema as list, options as dictionary
- **Returns**: `CreateExternalTable` AST node
- **Features**: Flexible schema definition, comprehensive options support

#### `b.export_data(options, as_query)`
- **Validation**: Non-empty options dictionary, as_query must be Select
- **Returns**: `ExportData` AST node
- **Features**: Full export options support with validation

## Implementation Details

### Pattern Compliance
- All AST nodes use `@dataclass` decorator
- All nodes inherit from appropriate base classes (Statement/Expression)
- All nodes implement `accept(self, visitor)` method
- All use `field(default_factory=...)` for mutable defaults
- All follow established naming conventions

### Visitor Pattern Support
- Added abstract visitor methods to `ASTVisitor` base class
- Implemented corresponding methods in `SQLSerializer`
- All new nodes can be serialized to valid BigQuery SQL

### Validation Features
- Comprehensive input validation in all builders
- Type checking for parameters
- Meaningful error messages with `ValidationError`
- Proper handling of complex data types (lists, dictionaries)

### Bug Fixes Included
- Fixed missing `@dataclass` decorator on `CreateTable`
- Fixed `TableName` serializer bug where string fields were incorrectly calling `accept()`

## Testing

### Comprehensive Test Suite
- Created `test_ml_external_tables.py` with focused tests
- Tests cover all new AST nodes and builders
- Validation error testing for edge cases
- Visitor pattern verification
- SQL serialization validation

### Demo Script
- Created `demo_ml_external.py` showcasing functionality
- Demonstrates realistic BigQuery ML and External Table use cases
- Shows generated SQL output

## SQL Output Examples

### CREATE MODEL
```sql
CREATE MODEL my_dataset.customer_model 
OPTIONS(model_type='logistic_reg', input_label_cols='["label"]', max_iterations=50)
```

### ML.PREDICT
```sql
ML.PREDICT(MODEL my_dataset.model, input_table, STRUCT(0.7 AS threshold))
```

### CREATE EXTERNAL TABLE
```sql
CREATE OR REPLACE EXTERNAL TABLE dataset.external_table 
(id INT64, name STRING) 
OPTIONS(format='CSV', uris='["gs://bucket/file.csv"]', skip_leading_rows=1)
```

### EXPORT DATA
```sql
EXPORT DATA OPTIONS(uri='gs://bucket/export.csv', format='CSV') 
AS SELECT * FROM table
```

## Compatibility
- All existing tests continue to pass
- No breaking changes to existing API
- Follows established repository patterns
- Compatible with existing visitor implementations