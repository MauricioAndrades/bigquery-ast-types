#!/usr/bin/env python3
"""
Type-Aware SQL Transformation Example

Demonstrates using BigQuery type information to create
more intelligent SQL transformations.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import List, Dict, Optional, Tuple
from sqlglot import exp, parse_one
from dataclasses import dataclass

from jsql import jsql
from bq_types import BigQueryType, BQDataType, TypeParser
from type_integration import TypeInferrer, TableSchema, TypedColumn, TypeEnforcer


@dataclass
class TypedMergeConfig:
    """Configuration for type-aware merge operations."""
    table_schema: TableSchema
    key_columns: List[str]
    update_columns: List[str]
    partition_column: Optional[str] = None
    order_column: Optional[str] = None
    
    def validate(self) -> None:
        """Validate configuration against schema."""
        all_columns = {col.name for col in self.table_schema.columns}
        
        # Check key columns exist
        for key in self.key_columns:
            if key not in all_columns:
                raise ValueError(f"Key column '{key}' not found in schema")
        
        # Check update columns exist
        for col in self.update_columns:
            if col not in all_columns:
                raise ValueError(f"Update column '{col}' not found in schema")
        
        # Check partition column
        if self.partition_column and self.partition_column not in all_columns:
            raise ValueError(f"Partition column '{self.partition_column}' not found in schema")
        
        # Check order column
        if self.order_column and self.order_column not in all_columns:
            raise ValueError(f"Order column '{self.order_column}' not found in schema")


class TypeAwareMergeBuilder:
    """Builds type-aware MERGE statements."""
    
    def __init__(self, config: TypedMergeConfig):
        self.config = config
        self.config.validate()
        self.schema = config.table_schema
        self.enforcer = TypeEnforcer(strict=False)
    
    def build_merge(self, source_alias: str = "source", 
                   target_alias: str = "target") -> exp.Merge:
        """Build a type-aware MERGE statement."""
        # Build MERGE statement
        merge = exp.Merge(
            this=self._build_target_table(target_alias),
            using=self._build_source_subquery(source_alias),
            on=self._build_match_condition(source_alias, target_alias)
        )
        
        # Add WHEN MATCHED clause
        merge.args["expressions"] = [
            self._build_when_matched(source_alias, target_alias),
            self._build_when_not_matched(source_alias)
        ]
        
        return merge
    
    def _build_target_table(self, alias: str) -> exp.Table:
        """Build target table reference."""
        return exp.Table(
            this=exp.Identifier(this=self.schema.name),
            alias=exp.TableAlias(this=exp.Identifier(this=alias))
        )
    
    def _build_source_subquery(self, alias: str) -> exp.Subquery:
        """Build source subquery with deduplication."""
        # Get columns with types
        select_cols = []
        for col in self.schema.columns:
            select_cols.append(
                exp.Column(this=exp.Identifier(this=col.name))
            )
        
        # Build base select
        base_select = exp.Select(
            expressions=select_cols,
            from_=exp.From(
                this=exp.Table(this=exp.Identifier(this=f"staging_{self.schema.name}"))
            )
        )
        
        # Add deduplication if we have partition and order columns
        if self.config.partition_column and self.config.order_column:
            # Add ROW_NUMBER window function
            row_num = exp.Window(
                this=exp.Anonymous(this="ROW_NUMBER"),
                partition_by=[exp.Column(this=exp.Identifier(this=pc)) 
                             for pc in self.config.key_columns],
                order=exp.Order(
                    expressions=[
                        exp.Ordered(
                            this=exp.Column(this=exp.Identifier(this=self.config.order_column)),
                            desc=True
                        )
                    ]
                )
            )
            
            # Wrap in CTE with row number
            with_row_num = exp.Select(
                expressions=[
                    exp.Star(),
                    exp.Alias(
                        this=row_num,
                        alias=exp.Identifier(this="rn")
                    )
                ],
                from_=base_select.subquery()
            )
            
            # Filter to rn = 1
            deduped = exp.Select(
                expressions=[
                    exp.Column(this=exp.Identifier(this=col.name))
                    for col in self.schema.columns
                ],
                from_=with_row_num.subquery(),
                where=exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="rn")),
                    expression=exp.Literal(this="1", is_string=False)
                )
            )
            
            return deduped.subquery(alias=alias)
        else:
            return base_select.subquery(alias=alias)
    
    def _build_match_condition(self, source_alias: str, target_alias: str) -> exp.Expression:
        """Build null-safe match condition for key columns."""
        conditions = []
        
        for key_col in self.config.key_columns:
            # Get column type
            col_def = self.schema.get_column(key_col)
            if not col_def:
                continue
            
            source_col = exp.Column(
                table=exp.Identifier(this=source_alias),
                this=exp.Identifier(this=key_col)
            )
            target_col = exp.Column(
                table=exp.Identifier(this=target_alias),
                this=exp.Identifier(this=key_col)
            )
            
            # For nullable columns, use null-safe equality
            if col_def.nullable:
                # (source.col = target.col OR (source.col IS NULL AND target.col IS NULL))
                eq_cond = exp.Or(
                    this=exp.EQ(this=source_col.copy(), expression=target_col.copy()),
                    expression=exp.And(
                        this=exp.Is(this=source_col.copy(), expression=exp.Null()),
                        expression=exp.Is(this=target_col.copy(), expression=exp.Null())
                    )
                )
                conditions.append(exp.Paren(this=eq_cond))
            else:
                # Non-nullable, simple equality
                conditions.append(exp.EQ(this=source_col, expression=target_col))
        
        # Combine with AND
        if len(conditions) == 1:
            return conditions[0]
        else:
            result = conditions[0]
            for cond in conditions[1:]:
                result = exp.And(this=result, expression=cond)
            return result
    
    def _build_when_matched(self, source_alias: str, target_alias: str) -> exp.WhenMatched:
        """Build WHEN MATCHED THEN UPDATE clause."""
        set_items = []
        
        for col_name in self.config.update_columns:
            col_def = self.schema.get_column(col_name)
            if not col_def:
                continue
            
            target_col = exp.Column(this=exp.Identifier(this=col_name))
            source_col = exp.Column(
                table=exp.Identifier(this=source_alias),
                this=exp.Identifier(this=col_name)
            )
            
            # Type validation could go here
            set_items.append(exp.EQ(this=target_col, expression=source_col))
        
        return exp.WhenMatched(
            then=exp.Update(
                expressions=set_items
            )
        )
    
    def _build_when_not_matched(self, source_alias: str) -> exp.WhenNotMatched:
        """Build WHEN NOT MATCHED THEN INSERT clause."""
        # Insert all columns
        columns = []
        values = []
        
        for col in self.schema.columns:
            columns.append(exp.Column(this=exp.Identifier(this=col.name)))
            values.append(
                exp.Column(
                    table=exp.Identifier(this=source_alias),
                    this=exp.Identifier(this=col.name)
                )
            )
        
        return exp.WhenNotMatched(
            then=exp.Insert(
                this=exp.Schema(expressions=columns),
                expression=exp.Values(
                    expressions=[exp.Tuple(expressions=values)]
                )
            )
        )


class TypeValidatingTransformer:
    """Transformer that validates and enforces types."""
    
    def __init__(self, schemas: Dict[str, TableSchema]):
        self.schemas = schemas
        self.inferrer = TypeInferrer(schemas)
        self.enforcer = TypeEnforcer(strict=False)
    
    def add_type_casts(self, sql: str) -> str:
        """Add explicit casts where needed for type safety."""
        ast = parse_one(sql)
        
        # Find all comparisons
        for comparison in ast.find_all(exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE):
            left = comparison.left
            right = comparison.right
            
            # Infer types
            left_type = self.inferrer.infer_expression_type(left)
            right_type = self.inferrer.infer_expression_type(right)
            
            # If types don't match and we know both types
            if (left_type and right_type and 
                left_type.base_type != right_type.base_type):
                
                # Check if comparison is valid
                if not self.enforcer.validate_comparison(left_type, right_type):
                    # Need to cast one side
                    # Prefer casting to the "larger" type
                    target_type = TypeCaster.find_common_supertype(
                        [left_type.base_type, right_type.base_type]
                    )
                    
                    if target_type:
                        target_bq_type = BigQueryType(base_type=target_type)
                        
                        # Cast left side if needed
                        if left_type.base_type != target_type:
                            comparison.args["this"] = self.enforcer.enforce_column_type(
                                left, target_bq_type, left_type
                            )
                        
                        # Cast right side if needed
                        if right_type.base_type != target_type:
                            comparison.args["expression"] = self.enforcer.enforce_column_type(
                                right, target_bq_type, right_type
                            )
        
        return ast.sql(dialect="bigquery")


def create_order_schema() -> TableSchema:
    """Create a detailed order table schema."""
    return TableSchema(
        name="orders",
        columns=[
            TypedColumn("order_id", BigQueryType(base_type=BQDataType.INT64), nullable=False),
            TypedColumn("customer_id", BigQueryType(base_type=BQDataType.INT64)),
            TypedColumn("order_date", BigQueryType(base_type=BQDataType.DATE)),
            TypedColumn("order_timestamp", BigQueryType(base_type=BQDataType.TIMESTAMP)),
            TypedColumn("status", BigQueryType(base_type=BQDataType.STRING, 
                                             parameters=[TypeParameter("length", 20)])),
            TypedColumn("total_amount", BigQueryType(base_type=BQDataType.NUMERIC,
                                                   parameters=[TypeParameter("precision", 10),
                                                              TypeParameter("scale", 2)])),
            TypedColumn("discount_rate", BigQueryType(base_type=BQDataType.FLOAT64)),
            TypedColumn("items", BigQueryType(
                base_type=BQDataType.ARRAY,
                element_type=BigQueryType(
                    base_type=BQDataType.STRUCT,
                    fields=[
                        ("product_id", BigQueryType(base_type=BQDataType.INT64)),
                        ("quantity", BigQueryType(base_type=BQDataType.INT64)),
                        ("price", BigQueryType(base_type=BQDataType.NUMERIC))
                    ]
                )
            )),
            TypedColumn("shipping_address", BigQueryType(base_type=BQDataType.GEOGRAPHY)),
            TypedColumn("metadata", BigQueryType(base_type=BQDataType.JSON)),
            TypedColumn("created_at", BigQueryType(base_type=BQDataType.TIMESTAMP)),
            TypedColumn("updated_at", BigQueryType(base_type=BQDataType.TIMESTAMP)),
        ]
    )


if __name__ == "__main__":
    print("=== Type-Aware SQL Transformation Example ===\n")
    
    # Create schema
    order_schema = create_order_schema()
    
    # Configure merge
    merge_config = TypedMergeConfig(
        table_schema=order_schema,
        key_columns=["order_id"],
        update_columns=["status", "total_amount", "discount_rate", 
                       "items", "metadata", "updated_at"],
        partition_column="order_date",
        order_column="updated_at"
    )
    
    # Build type-aware merge
    builder = TypeAwareMergeBuilder(merge_config)
    merge_stmt = builder.build_merge()
    
    print("Generated Type-Aware MERGE Statement:")
    print("-" * 60)
    print(merge_stmt.sql(dialect="bigquery", pretty=True))
    
    # Example of type validation
    print("\n\nType Validation Example:")
    print("-" * 60)
    
    # SQL with potential type issues
    problematic_sql = """
    SELECT 
        order_id,
        total_amount,
        CASE 
            WHEN total_amount > '100' THEN 'High'
            WHEN discount_rate > 0.5 THEN 'Discounted'
            ELSE 'Regular'
        END as category
    FROM orders
    WHERE order_date = '2024-01-01'
      AND customer_id = '12345'
    """
    
    print("Original SQL (with type issues):")
    print(problematic_sql)
    
    # Apply type validation and casting
    schemas = {"orders": order_schema}
    validator = TypeValidatingTransformer(schemas)
    
    fixed_sql = validator.add_type_casts(problematic_sql)
    
    print("\nFixed SQL (with type casts):")
    print(fixed_sql)
    
    print("\n✅ Type-aware transformations complete! 🐕")