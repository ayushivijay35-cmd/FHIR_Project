from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, md5, concat_ws, current_date
from pyspark.sql.types import StringType, BooleanType, DateType
from datetime import datetime, date
from typing import List, Optional


class SCDType2Handler:
    """Handler for implementing SCD Type 2 logic"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def calculate_record_hash(self, df: DataFrame, columns_to_hash: List[str]) -> DataFrame:
        """Calculate MD5 hash for change detection"""
        existing_cols = [c for c in columns_to_hash if c in df.columns]
        if not existing_cols:
            raise ValueError(f"No valid columns for hashing")
        
        return df.withColumn(
            "record_hash",
            md5(concat_ws("||", *[col(c).cast(StringType()) for c in existing_cols]))
        )
    
    def add_scd_columns(
        self, 
        df: DataFrame, 
        is_current: bool = True, 
        valid_from: Optional[date] = None
    ) -> DataFrame:
        """Add SCD Type 2 tracking columns"""
        if valid_from is None:
            valid_from = datetime.now().date()
        
        df = df.withColumn("is_current", lit(is_current).cast(BooleanType()))
        df = df.withColumn("valid_from", lit(valid_from).cast(DateType()))
        df = df.withColumn("valid_to", lit(None).cast(DateType()))
        
        return df
    
    def merge_scd_type2(
        self,
        target_table: str,
        source_df: DataFrame,
        key_columns: List[str],
        hash_columns: List[str],
        database: str = "fhir_lakehouse"
    ) -> dict:
        """Merge source data into target with SCD Type 2 logic"""
        full_table_name = f"{database}.{target_table}"
        
        # Check if table exists
        if not self._table_exists(database, target_table):
            print(f"  Performing initial load to {target_table}")
            return self._initial_load(source_df, full_table_name, hash_columns)
        
        print(f"  Performing incremental merge to {target_table}")
        
        target_df = self.spark.table(full_table_name).filter(col("is_current") == True)
        
        source_with_hash = self.calculate_record_hash(source_df, hash_columns)
        
        if "record_hash" not in target_df.columns:
            target_df = self.calculate_record_hash(target_df, hash_columns)
        
        # Join and classify records
        join_condition = [col(f"source.{k}") == col(f"target.{k}") for k in key_columns]
        joined_df = source_with_hash.alias("source").join(
            target_df.alias("target"), join_condition, "left"
        )
        
        new_records = joined_df.filter(col(f"target.{key_columns[0]}").isNull())
        
        changed_records = joined_df.filter(
            (col(f"target.{key_columns[0]}").isNotNull()) & 
            (col("source.record_hash") != col("target.record_hash"))
        )
        
        unchanged_records = joined_df.filter(
            (col(f"target.{key_columns[0]}").isNotNull()) & 
            (col("source.record_hash") == col("target.record_hash"))
        )
        
        # Calculate stats
        stats = {
            "new_records": new_records.count(),
            "changed_records": changed_records.count(),
            "unchanged_records": unchanged_records.count(),
            "timestamp": datetime.now()
        }
        
        print(f"  Stats: {stats['new_records']} new | {stats['changed_records']} changed | {stats['unchanged_records']} unchanged")
        
        # Process new records
        if stats["new_records"] > 0:
            new_df = new_records.select("source.*")
            new_df = self.add_scd_columns(new_df, is_current=True)
            new_df.write.format("delta").mode("append").saveAsTable(full_table_name)
            print(f"  ✓ Inserted {stats['new_records']} new records")
        
        # Process changed records
        if stats["changed_records"] > 0:
            self._expire_records(full_table_name, changed_records, key_columns)
            changed_df = changed_records.select("source.*")
            changed_df = self.add_scd_columns(changed_df, is_current=True)
            changed_df.write.format("delta").mode("append").saveAsTable(full_table_name)
            print(f"  ✓ Updated {stats['changed_records']} records (old expired, new inserted)")
        
        return stats
    
    def _initial_load(self, df: DataFrame, table_name: str, hash_columns: List[str]) -> dict:
        """Initial load to new table"""
        df_with_hash = self.calculate_record_hash(df, hash_columns)
        df_with_scd = self.add_scd_columns(df_with_hash, is_current=True)
        count = df_with_scd.count()
        df_with_scd.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"  ✓ Initial load: {count} records")
        return {
            "new_records": count,
            "changed_records": 0,
            "unchanged_records": 0,
            "timestamp": datetime.now()
        }
    
    def _expire_records(self, table_name: str, changed_df: DataFrame, key_columns: List[str]):
        """Expire old versions"""
        from delta.tables import DeltaTable
        
        delta_table = DeltaTable.forName(self.spark, table_name)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
        merge_condition += " AND target.is_current = true"
        
        source_keys = changed_df.select([f"source.{k}" for k in key_columns]).distinct()
        
        delta_table.alias("target").merge(
            source_keys.alias("source"), merge_condition
        ).whenMatchedUpdate(
            set={"is_current": "false", "valid_to": "current_date()"}
        ).execute()
    
    def _table_exists(self, database: str, table: str) -> bool:
        try:
            self.spark.table(f"{database}.{table}")
            return True
        except:
            return False