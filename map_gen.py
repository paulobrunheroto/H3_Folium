from h3_folium.h3_python import PythonH3
from h3_folium.h3_spark import SparkH3
import pandas as pd
from pyspark.sql import SparkSession
import time
import json

input_setup = json.load(open("setup.json"))

start_time = time.time()
print(f"Process initiated for: {input_setup['mode']}")

if input_setup['mode'] == "pandas":
    df = pd.read_csv(input_setup['file'])
    PythonH3(**input_setup).map_gen(df, 'map_gen')
else:
    spark = SparkSession.builder.appName("SparkH3").getOrCreate()
    df = spark.read.csv(input_setup['file'], header=True)
    SparkH3(**input_setup).map_gen(df, 'map_gen')

print("--- %s seconds ---" % (time.time() - start_time))
input("Completed! Press enter to exit...")
