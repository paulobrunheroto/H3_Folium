import folium
from geojson import Feature, FeatureCollection
import json
import h3
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F
import branca
import math

class h3_map():
    def __init__(self, mode = 'pandas'):
        if mode not in ['pandas','spark']:
            raise TypeError('Mode should be one of the following: pandas, spark')
        self.mode = mode

    def to_hex_id(self, df, lat_col, lng_col, resolution=3):
        if self.mode == 'pandas':
            df["hex_id"] = df.apply(lambda row: h3.geo_to_h3(row[lat_col], row[lng_col], resolution), axis=1)
        else:
            spark_hex = F.udf(lambda a,b: h3.geo_to_h3(a, b, resolution), returnType=StringType())
            df = df.withColumn('hex_id', spark_hex(df[lat_col].cast(DoubleType()),df[lng_col].cast(DoubleType())))
        return df
    
    def df_to_geojson(self, df_hex):
        if self.mode == 'pandas':
            return self.pandas_to_geojson(df_hex)
        else:
            return self.spark_to_geojson(df_hex)

    def pandas_to_geojson(self, df_hex):
        geojson_feature = lambda row: Feature(
            geometry = {"type" : "Polygon", "coordinates": [h3.h3_to_geo_boundary(h=row['hex_id'],geo_json=True)]},
            id = row['hex_id'],
            properties = {'value': row['value']}
            )
        df_hex['feature'] = df_hex.apply(geojson_feature, axis=1)
        return json.dumps(FeatureCollection(df_hex['feature'].to_list()))
    
    def spark_to_geojson(self, df_hex):
        geojson_feature = F.udf(lambda hex_id, hex_value: json.dumps(Feature(
            geometry = {"type" : "Polygon", "coordinates": [h3.h3_to_geo_boundary(h=hex_id,geo_json=True)]},
            id = hex_id,
            properties = {'value': hex_value}
            )))
        df_hex = df_hex.withColumn('feature', geojson_feature(df_hex['hex_id'],df_hex['value']))
        return json.dumps(FeatureCollection(df_hex.rdd.map(lambda x: json.loads(x['feature'])).collect()))
    
    def get_statistics(self, df):
        if self.mode == 'pandas':
            return df['value'].min(), df['value'].max(), df['value'].mean()
        else:
            stats = df.select(F.max(df['value']).alias('max'), F.min(df['value']).alias('min'), F.mean(df['value']).alias('mean')).collect()
            return stats[0]['min'], stats[0]['max'], stats[0]['mean']

    def h3_folium_map(self, df_hex, steps = None):
        """
        Creates choropleth maps given the aggregated data. initial_map can be an existing map to draw on top of.
        """    
        #colormap
        min_value, max_value, mean_value = self.get_statistics(df_hex)
        # delta = max_value - min_value
        # to_opacity = lambda x: max_opacity if delta==0 else (x-min_value)/delta*(max_opacity-min_opacity)+min_opacity
        print(f"Colour column min value {min_value}, max value {max_value}, mean value {mean_value}")
        
        initial_map = folium.Map(location= [47, 4], zoom_start=5.5, tiles="cartodbpositron")

        # colormap = matplotlib.colors.LogNorm(min_value, max_value)
        colormap = branca.colormap.linear.YlOrRd_09.scale(0, math.ceil(max_value/10)*10)
        if steps != None:
            colormap = colormap.to_step(steps)
        colormap.caption = 'Legenda do gráfico'
        colormap.add_to(initial_map)

        #create geojson data from dataframe
        geojson_data = self.df_to_geojson(df_hex = df_hex)

        folium.GeoJson(
            geojson_data,
            tooltip=folium.GeoJsonTooltip(fields=['value'], aliases=['count']),
            style_function = lambda feature: {
                'fillColor': colormap(feature["properties"]['value']),
                'weight': 1,
                'fillOpacity': 0.3#to_opacity(feature["properties"]['value'])
            }, 
            name = 'hex'
        ).add_to(initial_map)

        return initial_map

                

# def to_geojson_feature(df:Iterable[Dict[str,Any]]) -> Iterable[Dict[str,Any]]:
#     for row in df:
#         hex_id = row["hex_id"]
#         geometry = {"type" : "Polygon", "coordinates": [h3.h3_to_geo_boundary(hex_id, geo_json=True)]}
#         feature = Feature(geometry=geometry, properties={"hex_value":row["hex_value"]}, id=hex_id)
#         row["geojson_feature"] = str(feature)
#         yield row

# def hexagons_dataframe_to_geojson(df_hex, column_name = "value"):
#     """
#     Produce the GeoJSON for a dataframe, constructing the geometry from the "hex_id" column
#     and with a property matching the one in column_name
#     """    
#     list_features = []
    
#     for i,row in df_hex.iterrows():
#         try:
#             geometry_for_row = { "type" : "Polygon", "coordinates": [h3.h3_to_geo_boundary(h=row["hex_id"],geo_json=True)]}
#             feature = Feature(geometry = geometry_for_row , id=row["hex_id"], properties = {column_name : row[column_name]})
#             list_features.append(feature)
#         except Exception as e:
#             print("An exception occurred for hex " + row["hex_id"])
#             print(e)

#     feat_collection = FeatureCollection(list_features)
#     geojson_result = json.dumps(feat_collection)
#     return geojson_result


# def choropleth_map(df_aggreg, column_name = "value", color = 'blue', min_opacity=0.2, max_opacity = 0.7):
#     """
#     Creates choropleth maps given the aggregated data. initial_map can be an existing map to draw on top of.
#     """    
#     #colormap
#     min_value = df_aggreg[column_name].min()
#     max_value = df_aggreg[column_name].max()
#     mean_value = df_aggreg[column_name].mean()
#     delta = max_value - min_value
#     to_opacity = lambda x: max_opacity if delta==0 else (x-min_value)/delta*(max_opacity-min_opacity)+min_opacity
#     print(f"Colour column min value {min_value}, max value {max_value}, mean value {mean_value}")
#     print(f"Hexagon cell count: {df_aggreg['hex_id'].nunique()}")
    
#     # the name of the layer just needs to be unique, put something silly there for now:
#     name_layer = "Choropleth " + str(df_aggreg)
    
#     initial_map = folium.Map(location= [47, 4], zoom_start=5.5, tiles="cartodbpositron")

#     #create geojson data from dataframe
#     geojson_data = hexagons_dataframe_to_geojson(df_hex = df_aggreg, column_name = column_name)

#     folium.GeoJson(
#         geojson_data,
#         style_function=lambda feature: {
#             'fillColor': color,
#             'weight': 1,
#             'fillOpacity': to_opacity(feature["properties"][column_name])
#         }, 
#         name = name_layer
#     ).add_to(initial_map)

#     return initial_map