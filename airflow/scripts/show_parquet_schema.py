import pyarrow.parquet as pq

file_path = 'yellow_tripdata_2025-01.parquet'

# Read the schema
schema = pq.read_schema(file_path)

# Print the schema (this lists the column names and their types)
print(schema, sep='\n')

# To get just the column names as a list:
column_names = schema.names
print(f"\nColumn names: {column_names}")