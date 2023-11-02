# H3 Folium
Library for plotting maps using Folium + H3 in both Python and PySpark

## How to use?
There are two options that can be selected, either the Python or PySpark version using the following import
```python
from h3_folium.h3_python import PythonH3 # Python version
from h3_folium.h3_spark import SparkH3 # Spark version
```
For both modules, three parameters must be provided:
```python
ph3 = PythonH3(lat_col='lat', long_col='long', resolution=4)
sh3 = SparkH3(lat_col='lat', long_col='long', resolution=4)
```
- lat_col: Name of the latitude column of the dataframe
- long_col: Name of the Longitude column of the dataframe
- resolution: Resolution to be used for the Hexagons (default: 3)

Once the module is imported, the functions can be used separetly for a more custom result, or as exemplified below for a simple count map with the default configuration:
```python
# Python version
from h3_folium.h3_python import PythonH3
import pandas as pd

ph3 = PythonH3(lat_col='lat', long_col='long', resolution=4)

df = pd.read_csv("input_files/french_locations.csv")

ph3.map_gen(df, 'pandas_folium')
```
```python
# Spark version
from h3_folium.h3_python import SparkH3
from pyspark.sql import SparkSession

sh3 = SparkH3(lat_col='lat', long_col='long', resolution=4)

spark = SparkSession.builder.appName('SparkH3').getOrCreate()
df = spark.read.csv("input_files/french_locations.csv", header=True)

ph3.map_gen(df, 'spark_folium')
```
