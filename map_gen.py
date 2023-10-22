from h3_map import h3_map
import pandas as pd
from pyspark.sql import SparkSession
import time

mode = None
while mode not in ['pandas','spark']:
    mode = input('Select processing mode (pandas or spark): ').lower()

h3 = h3_map(mode=mode)

file = 'french_locations.csv'#input('Input file name: ')
resolution = 4#input('Input resolution: ')
lat = 'lat'#input('Input latitude column name: ')
long = 'long'#input('Input longitude column name: ')

start_time = time.time()
print(f'Process initiated for: {mode}')

if mode == 'pandas':
    df = pd.read_csv(file)
    df_hex = h3.to_hex_id(df, lat_col=lat, lng_col=long, resolution=int(resolution))
    df_agg = df_hex.groupby('hex_id').count().reset_index()
    df_agg.rename(columns={lat:'value'}, inplace=True)
    df_agg = df_agg[['hex_id','value']]
else:
    spark = SparkSession.builder.appName('H3_Folium').getOrCreate()
    df = spark.read.csv(file, header=True)
    df_hex = h3.to_hex_id(df, lat_col=lat, lng_col=long, resolution=int(resolution))
    df_agg = df_hex.groupBy('hex_id').count().withColumnRenamed('count','value')

map_folium = h3.h3_folium_map(df_agg)
map_folium.save('h3_folium.html')

print("--- %s seconds ---" % (time.time() - start_time))
input('Completed! Press enter to exit...')