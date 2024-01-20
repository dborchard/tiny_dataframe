### Data Generation

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Creating a DataFrame with specified data
df = pd.DataFrame({
    'c1': [100, 100, 100, 200,200, 300],
    'c2': [101, 201, 301, 401, 501, 601],
    'c3': [102, 202, 302, 402, 502, 602]
}, dtype='int64') 

# Convert the DataFrame to a PyArrow Table
table = pa.Table.from_pandas(df)

# Save the table as a Parquet file
pq.write_table(table, 'c1_c2_c3_int64.parquet')
```
### Data Reading

```python
import pyarrow.parquet as pq

# Read the Parquet file
table_read = pq.read_table('sample_int32.parquet')

# Convert to a pandas DataFrame
df_read = table_read.to_pandas()

# Display the DataFrame
print(df_read.head())
```