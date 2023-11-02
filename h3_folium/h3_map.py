import folium
import json
import h3
import branca
import math
import numpy as np


class MapH3:
    def __init__(self, lat_col, long_col, resolution):
        self.lat_col = lat_col
        self.lng_col = long_col
        self.resolution = resolution

    def h3_folium_map(self, geojson_data, steps=None, name="hex"):
        """
        Creates choropleth maps given the aggregated data. initial_map can be an existing map to draw on top of.
        """
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
            tiles=folium.TileLayer("cartodbpositron", name="CartoDB Positron"),
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
            tooltip=folium.GeoJsonTooltip(
                fields=["value"], aliases=[f"Count of {name}"]
            ),
            style_function=lambda feature: {
                "fillColor": colormap(feature["properties"]["value"]),
                "weight": 0.2,
                "fillOpacity": 0.5,
            },
            name=name,
        ).add_to(initial_map)

        folium.TileLayer("cartodbdark_matter", name="CartoDB Dark Matter").add_to(
            initial_map
        )
        folium.TileLayer("openstreetmap", name="Open Street Map").add_to(initial_map)
        folium.LayerControl(collapsed=False).add_to(initial_map)

        return initial_map
