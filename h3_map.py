import folium
from geojson import Feature, FeatureCollection
import json
import h3
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F
import branca
import math
import numpy as np


class h3_map:
    def __init__(self, mode="pandas"):
        if mode not in ["pandas", "spark"]:
            raise TypeError("Mode should be one of the following: pandas, spark")
        self.mode = mode

    def to_hex_id(self, df, lat_col, lng_col, resolution=3):
        if self.mode == "pandas":
            df["hex_id"] = df.apply(
                lambda row: h3.geo_to_h3(row[lat_col], row[lng_col], resolution), axis=1
            )
        else:
            spark_hex = F.udf(
                lambda a, b: h3.geo_to_h3(a, b, resolution), returnType=StringType()
            )
            df = df.withColumn(
                "hex_id",
                spark_hex(
                    df[lat_col].cast(DoubleType()), df[lng_col].cast(DoubleType())
                ),
            )
        return df

    def df_to_geojson(self, df_hex):
        if self.mode == "pandas":
            return self.pandas_to_geojson(df_hex)
        else:
            return self.spark_to_geojson(df_hex)

    def pandas_to_geojson(self, df_hex):
        geojson_feature = lambda row: Feature(
            geometry={
                "type": "Polygon",
                "coordinates": [h3.h3_to_geo_boundary(h=row["hex_id"], geo_json=True)],
            },
            id=row["hex_id"],
            properties={"value": row["value"]},
        )
        df_hex["feature"] = df_hex.apply(geojson_feature, axis=1)
        return FeatureCollection(df_hex["feature"].to_list())

    def spark_to_geojson(self, df_hex):
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

    def h3_folium_map(self, df_hex, steps=None, name = 'hex'):
        """
        Creates choropleth maps given the aggregated data. initial_map can be an existing map to draw on top of.
        """
        # create geojson data from dataframe
        geojson_data = self.df_to_geojson(df_hex=df_hex)

        points = ()
        values = []
        for feature in geojson_data["features"]:
            points += h3.h3_to_geo_boundary(feature["id"])
            values += [feature["properties"]["value"]]

        # colormap
        min_value = min(values)
        max_value = max(values)
        mean_value = np.mean(values)
        print(
            f"Colour column min value {min_value}, max value {max_value}, mean value {mean_value}"
        )

        initial_map = folium.Map(
            location=[np.mean([x[0] for x in points]), np.mean([x[1] for x in points])],
            zoom_start=5.5,
            tiles = folium.TileLayer("cartodbpositron", name= 'CartoDB Positron')
        )

        colormap = branca.colormap.linear.YlOrRd_09.scale(
            0, math.ceil(max_value / 10) * 10
        )
        if steps != None:
            colormap = colormap.to_step(steps)
        colormap.caption = f"Count of {name}"
        colormap.add_to(initial_map)

        folium.GeoJson(
            json.dumps(geojson_data),
            tooltip=folium.GeoJsonTooltip(fields=["value"], aliases=[f"Count of {name}"]),
            style_function=lambda feature: {
                "fillColor": colormap(feature["properties"]["value"]),
                "weight": 0.2,
                "fillOpacity": 0.5,
            },
            name=name,
        ).add_to(initial_map)
        
        folium.TileLayer('cartodbdark_matter', name='CartoDB Dark Matter').add_to(initial_map)
        folium.TileLayer('openstreetmap', name='Open Street Map').add_to(initial_map)
        folium.LayerControl(collapsed=False).add_to(initial_map)

        return initial_map
