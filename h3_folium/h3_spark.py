from geojson import Feature, FeatureCollection
import json
import h3
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F
from h3_folium.h3_map import MapH3

class SparkH3(MapH3):
    def __init__(self, lat_col, long_col, resolution = 3, **kwargs):
        super().__init__(lat_col, long_col, resolution)

    def to_hex_id(self, df):
        spark_hex = F.udf(
            lambda a, b: h3.geo_to_h3(a, b, self.resolution), returnType=StringType()
        )
        df = df.withColumn(
            "hex_id",
            spark_hex(
                df[self.lat_col].cast(DoubleType()), df[self.lng_col].cast(DoubleType())
            ),
        )
        return df

    def df_to_geojson(self, df_hex):
        geojson_feature = F.udf(
            lambda hex_id, hex_value: json.dumps(
                Feature(
                    geometry={
                        "type": "Polygon",
                        "coordinates": [h3.h3_to_geo_boundary(h=hex_id, geo_json=True)],
                    },
                    id=hex_id,
                    properties={"value": hex_value},
                )
            )
        )
        df_hex = df_hex.withColumn(
            "feature", geojson_feature(df_hex["hex_id"], df_hex["value"])
        )
        return FeatureCollection(
            df_hex.rdd.map(lambda x: json.loads(x["feature"])).collect()
        )
    
    def map_gen(self, df, file_name='h3_folium'):
        df_hex = self.to_hex_id(df)

        df_agg = df_hex.groupBy("hex_id").count().withColumnRenamed("count", "value")

        geojson_data = self.df_to_geojson(df_agg)
        map_folium = self.h3_folium_map(geojson_data, name = 'Visits')
        map_folium.save(f"output_files/{file_name}.html")
        return map_folium