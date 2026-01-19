#!/usr/bin/env python3
"""Debug script to check what schema is being generated for Person."""

from pyspark.sql import SparkSession
from tests.utils.sample_data_generator import generate_sample_data_for_yaml
from tests.utils.example_loader import load_yaml_examples
from pathlib import Path

# Create Spark session
spark = SparkSession.builder \
    .appName("SchemaDebug") \
    .master("local[1]") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

# Load features_queries.yaml
yaml_path = Path("examples/features_queries.yaml")
yaml_data, _ = load_yaml_examples(yaml_path)

print("=== YAML Schema for Person ===")
for node in yaml_data.get("schema", {}).get("nodes", []):
    if node["name"] == "Person":
        print(f"Properties defined in YAML:")
        for prop in node.get("properties", []):
            print(f"  - {prop['name']}: {prop.get('type', 'string')}")

# Generate sample data
generator = generate_sample_data_for_yaml(
    spark,
    yaml_data,
    default_node_count=3,
    seed=42,
)

# Check the Person DataFrame (using full table name)
# Find Person node's table name from schema
person_table_name = None
for node in yaml_data.get("schema", {}).get("nodes", []):
    if node["name"] == "Person":
        person_table_name = node.get("tableName", "Person")
        break

if person_table_name:
    person_df = generator.get_dataframe(person_table_name)
    if person_df:
        print(f"\n=== Generated Person DataFrame Schema (from {person_table_name}) ===")
        person_df.printSchema()
        print("\n=== Sample Data ===")
        person_df.show(truncate=False)

    # Check if view exists (views now use full table names)
    try:
        view_df = spark.sql(f"SELECT * FROM {person_table_name}")
        print(f"\n=== Person View Schema (from view {person_table_name}) ===")
        view_df.printSchema()
        print("\n=== Person View Sample Data ===")
        view_df.show(truncate=False)
    except Exception as e:
        print(f"\n=== Error reading Person view: {e} ===")

spark.stop()
