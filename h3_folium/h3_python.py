from geojson import Feature, FeatureCollection
import h3
from h3_folium.h3_map import MapH3

class PythonH3(MapH3):
    def __init__(self, lat_col, long_col, resolution = 3, **kwargs):
        super().__init__(lat_col, long_col, resolution)

    def to_hex_id(self, df):
        df["hex_id"] = df.apply(
                lambda row: h3.geo_to_h3(row[self.lat_col], row[self.lng_col], self.resolution), axis=1
            )
        return df

    def df_to_geojson(self, df_hex):
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
    
    def map_gen(self, df,file_name='h3_folium'):
        df_hex = self.to_hex_id(df)

        df_agg = df_hex.groupby("hex_id").count().reset_index()
        df_agg.rename(columns={self.lat_col: "value"}, inplace=True)
        df_agg = df_agg[["hex_id", "value"]]

        geojson_data = self.df_to_geojson(df_agg)
        map_folium = self.h3_folium_map(geojson_data, name = 'Visits')
        map_folium.save(f"output_files/{file_name}.html")
        return map_folium
