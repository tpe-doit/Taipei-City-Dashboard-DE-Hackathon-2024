import geopandas as gpd
import pandas as pd
from geoalchemy2 import WKTElement
from numpy import nan
from shapely.geometry import MultiLineString
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon


def convert_3d_polygon_to_2d_polygon(geo):
    """
    Convert 3D Multi/Polygons to 2D Multi/Polygons.
    
    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import geopandas as gpd
        from shapely.geometry import Polygon
        from utils.transform_geometry import convert_3d_polygon_to_2d_polygon

        polygon_with_z = Polygon([(0, 0, 0), (1, 0, 1), (1, 1, 2), (0, 1, 3), (0, 0, 0)])
        geos_polyz = gpd.GeoSeries([polygon_with_z, polygon_with_z])
        print(geos_polyz)
        # >>> print(geos_polyz)
        # 0    POLYGON Z ((0.00000 0.00000 0.00000, 1.00000 0...
        # 1    POLYGON Z ((0.00000 0.00000 0.00000, 1.00000 0...
        # dtype: geometry

        geos_poly = geos_polyz.apply(convert_3d_polygon_to_2d_polygon)
        print(geos_poly)
        # >>> print(geos_poly)
        # 0    POLYGON ((0.00000 0.00000, 1.00000 0.00000, 1....
        # 1    POLYGON ((0.00000 0.00000, 1.00000 0.00000, 1....
        # dtype: geometry
        ```
    """
    if geo.has_z:
        if geo.geom_type == "Polygon":
            lines = [xy[:2] for xy in list(geo.exterior.coords)]
            new_geo = Polygon(lines)
        elif geo.geom_type == "MultiPolygon":
            new_multi_p = []
            for ap in geo:
                lines = [xy[:2] for xy in list(ap.exterior.coords)]
                new_p = Polygon(lines)
                new_multi_p.append(new_p)
            new_geo = MultiPolygon(new_multi_p)
    return new_geo


def convert_linestring_to_multilinestring(geo):
    """
    Convert LineString to MultiLineString.

    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)

        import geopandas as gpd
        from shapely.geometry import LineString
        from utils.transform_geometry import convert_linestring_to_multilinestring

        geos = gpd.GeoSeries([LineString([[0,0], [1,1]]), LineString([[0,0], [1,1]])])

        geos_mline = geos.apply(convert_linestring_to_multilinestring)
        print(geos_mline)
        ```
        ```
        >>> print(geos_mline)
        0    MULTILINESTRING ((0.00000 0.00000, 1.00000 1.0...
        1    MULTILINESTRING ((0.00000 0.00000, 1.00000 1.0...
        dtype: geometry
        ```
    """
    is_multistring = isinstance(geo, MultiLineString)
    is_na = pd.isna(geo)
    if (is_multistring) or (is_na):
        return geo
    else:
        return MultiLineString([geo])


def convert_polygon_to_multipolygon(geo):
    """
    Convert Polygon to MultiPolygon.

    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)

        import geopandas as gpd
        from shapely.geometry import LineString
        from utils.transform_geometry import convert_polygon_to_multipolygon

        poly = Polygon([[0,0], [1,1], [1,0]])
        geos = gpd.GeoSeries([poly, poly])
        geos_mpoly = geos.apply(convert_polygon_to_multipolygon)
        print(geos_mpoly)
        ```
        ```
        >>> print(geos_mpoly)
        0    MULTIPOLYGON (((0.00000 0.00000, 1.00000 1.000...
        1    MULTIPOLYGON (((0.00000 0.00000, 1.00000 1.000...
        dtype: geometry
        ```
    """
    is_multipolygon = isinstance(geo, MultiPolygon)
    is_na = pd.isna(geo)
    if (is_multipolygon) or (is_na):
        return geo
    else:
        return MultiPolygon([geo])


def add_point_wkbgeometry_column_to_df(
    data: pd.DataFrame,
    x: pd.Series,
    y: pd.Series,
    from_crs: int,
    to_crs=4326,
    is_add_xy_columns=True,
) -> gpd.GeoDataFrame:
    """
    Convert original DataFrame with x and y to GeoDataFrame with wkbgeometry.
    Input should be a pandas.DataFrame.
    Output will be a geopandas.GeoDataFrame and add 3 columns - wkb_geometry, lng, lat.

    Parameters
    ----------
    df: input DataFrame.
    x: x or lng.
    y: y or lat.
    from_crs: crs alias name as EPSG, commonly use 4326=WGS84 or 3826=TWD97
    to_crs: crs alias name as EPSG, default 4326.
    is_add_xy_columns: Add lng(x), lat(y) to output, defalut True.
        Only point type geometry will add column.
        Add these two column can benifit powerBI user.

    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import pandas as pd
        import geopandas as gpd
        from utils.transform_geometry import add_point_wkbgeometry_column_to_df

        data = pd.DataFrame({
            'id': [1, 2],
            'attribute': ['A', 'B']
        })
        x = pd.Series([262403.2367, None])
        y = pd.Series([2779407.0527, ''])
        gdf = add_point_wkbgeometry_column_to_df(data, x, y, from_crs=3826)
        print(gdf.iloc[0])
        ```
        ```
        >>> print(gdf.iloc[0])
        id                                                          1
        attribute                                                   A
        geometry        POINT (121.12299999921674 25.123000193639967)
        lng                                                   121.123
        lat                                                    25.123
        wkb_geometry    POINT (121.12299999921674 25.123000193639967)
        Name: 0, dtype: object
        ```
    """
    # covert column type
    x = pd.to_numeric(x, errors="coerce")
    y = pd.to_numeric(y, errors="coerce")
    geometry = gpd.points_from_xy(x, y)

    # make df to gdf
    df = data.copy()
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=f"EPSG:{from_crs}")
    if from_crs == to_crs:
        gdf = gdf.to_crs(epsg=from_crs)
        gdf = gdf.to_crs(epsg=to_crs)
    else:
        gdf = gdf.to_crs(epsg=to_crs)

    # add column
    if is_add_xy_columns:
        geo_type = gdf["geometry"].type.iloc[0]
        if geo_type == "Point":
            gdf["lng"] = gdf["geometry"].map(
                lambda ele: ele.x if not ele.is_empty else nan
            )
            gdf["lat"] = gdf["geometry"].map(
                lambda ele: ele.y if not ele.is_empty else nan
            )
    gdf["wkb_geometry"] = gdf["geometry"].apply(
        lambda x: WKTElement(x.wkt, srid=to_crs) if x is not None else None
    )

    return gdf


def convert_geometry_to_wkbgeometry(
    gdf: gpd.GeoDataFrame, from_crs: int, to_crs=4326
) -> gpd.GeoDataFrame:
    """
    Convert geometry column of GeoDataframe to wkbgeometry.

    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import pandas as pd
        import geopandas as gpd
        from shapely.geometry import Polygon
        from utils.transform_geometry import convert_geometry_to_wkbgeometry

        data = pd.DataFrame({
            'id': [1, 2],
            'attribute': ['A', 'B']
        })
        poly = pd.Series([
            Polygon([[262403, 2779407], [262404, 2779407], [262404, 2779408]]),
            Polygon([[262403, 2779407], [262405, 2779407], [262404, 2779408]])
        ])
        gdf = gpd.GeoDataFrame(data, geometry=poly, crs='EPSG:3826')
        gdf = convert_geometry_to_wkbgeometry(gdf, from_crs=3826)
        print(gdf.iloc[0])
        ```
        ```
        >>> print(gdf.iloc[0])
        id                                                              1
        attribute                                                       A
        geometry        POLYGON ((121.12299765144614 25.12299971980088...
        wkb_geometry    POLYGON ((121.12299765144614 25.12299971980088...
        Name: 0, dtype: object
        ```
    """
    # to make sure gdf is in `from_crs` projection
    gdf.crs = f"EPSG:{from_crs}"

    if from_crs == to_crs:
        gdf = gdf.to_crs(epsg=from_crs)
        gdf = gdf.to_crs(epsg=to_crs)
    else:
        gdf = gdf.to_crs(epsg=to_crs)
    gdf["wkb_geometry"] = gdf["geometry"].apply(
        lambda x: WKTElement(x.wkt, srid=to_crs) if x is not None else None
    )

    return gdf
