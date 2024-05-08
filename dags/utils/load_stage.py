import time

import geopandas as gpd
from geoalchemy2 import Geometry
from sqlalchemy.sql import text as sa_text


def save_dataframe_to_postgresql(
    engine, data, load_behavior: str, default_table: str, history_table: str = None
):
    """
    Save pd.DataFrame to psql.
    A high-level API for pd.DataFrame.to_psql with `index=False` and `schema='public'`.
    If the data is gpd.GeoDataFrame, use function `save_geodataframe_to_postgresql` instead.

    Args:
    engine : sqlalchemy.engine.base.Engine.
    data : pd.DataFrame. Data to be saved.
    load_behavior : str. Save mode, should be one of `append`, `replace`, `current+history`.
        `append`: Just append new data to the `default_table`.
        `replace`: Truncate the `default_table` and append new data.
        `current+history`: The current+history design is intended to preserve historical records
            while simultaneously maintaining the most recent data. This proces will truncate
            the `default_table` and append the new data into it, then append the new data
            to `history_table`.
    default_table : str. Default table name.
    history_table : str. History table name, only used when load_behavior is `current+history`.
    """
    # check data type
    if isinstance(data, gpd.GeoDataFrame):
        raise ValueError(
            "Data type is gpd.GeoDataFrame, use function `save_geodataframe_to_postgresql` instead."
        )

    is_column_include_geometry = data.columns.isin(["wkb_geometry", "geometry"])
    if any(is_column_include_geometry):
        raise ValueError(
            """
            Column name contains `wkb_geometry` or `geometry`, it should be a GeoDataFrame.
            Please use function `save_geodataframe_to_postgresql` instead.
        """
        )

    start_time = time.time()

    # main
    conn = engine.connect()
    if load_behavior == "append":
        data.to_sql(
            default_table, conn, if_exists="append", index=False, schema="public"
        )
    elif load_behavior == "replace":
        conn.execute(sa_text(f"TRUNCATE TABLE {default_table}"))
        data.to_sql(
            default_table, conn, if_exists="append", index=False, schema="public"
        )
    elif load_behavior == "current+history":
        if history_table is None:
            raise ValueError(
                "history_table should be provided when load_behavior is `current+history`."
            )
        conn.execute(sa_text(f"TRUNCATE TABLE {default_table}"))
        data.to_sql(
            default_table, conn, if_exists="append", index=False, schema="public"
        )
        data.to_sql(
            history_table, conn, if_exists="append", index=False, schema="public"
        )
    else:
        raise ValueError(
            "load_behavior should be one of `append`, `replace`, `current+history`."
        )
    conn.commit()
    conn.close()

    # print
    cost_time = time.time() - start_time
    print(f"Data been saved, cost time: {cost_time:.2f}s.")


def save_geodataframe_to_postgresql(
    engine,
    gdata,
    load_behavior: str,
    geometry_type: str,
    default_table: str,
    history_table: str = None,
    geometry_col: str = "wkb_geometry",
):
    """
    Save gpd.GeoDataFrame to psql.
    A high-level API for gpd.GeoDataFrame.to_psql with `index=False` and `schema='public'`.
    The geometry column should be in WKB format, and EPSG:4326 is used.
    If the data is pd.DataFrame, use function `save_dataframe_to_postgresql` instead.

    Args:
    engine : sqlalchemy.engine.base.Engine.
    gdata : gpd.GeoDataFrame. Data with geometry to be saved.
    load_behavior : str. Save mode, should be one of `append`, `replace`, `current+history`.
        `append`: Just append new data to the `default_table`.
        `replace`: Truncate the `default_table` and append new data.
        `current+history`: The current+history design is intended to preserve historical records
            while simultaneously maintaining the most recent data. This proces will truncate
            the `default_table` and append the new data into it, then append the new data
            to `history_table`.
    default_table : str. Default table name.
    history_table : str. History table name, only used when load_behavior is `current+history`.
    geometry_type : str. Geometry type, should be one of the following:
        ['POINT', 'LINESTRING', 'POLYGON', 'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON'].
    geometry_col : str. The geometry column name. Default is 'wkb_geometry'.
        *The data in the column should be in WKB format.
        “Well-known binary” (WKB) is a scheme for writing a simple features geometry into a
        platform-independent array of bytes, usually for transport between systems or between
        programs. By using WKB, systems can avoid exposing their particular internal implementation
        of geometry storage, for greater overall interoperability.
    """
    # Data type should not been checked, because the process of geometry to wkb_geometry.
    # The process could generate invalid geometry, so data type cant be converted to GeoDataFrame.

    # check geometry type is valid
    white_list = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "LineStringZ",
        "MultiLineStringZ",
    ]
    if geometry_type not in white_list:
        raise ValueError(
            f"geometry_type should be one of {white_list}, but got {geometry_type}."
        )

    start_time = time.time()

    # main
    conn = engine.connect()
    if load_behavior == "append":
        gdata.to_sql(
            default_table,
            conn,
            if_exists="append",
            index=False,
            schema="public",
            dtype={geometry_col: Geometry(geometry_type, srid=4326)},
        )
    elif load_behavior == "replace":
        conn.execute(sa_text(f"TRUNCATE TABLE {default_table}"))
        gdata.to_sql(
            default_table,
            conn,
            if_exists="append",
            index=False,
            schema="public",
            dtype={geometry_col: Geometry(geometry_type, srid=4326)},
        )
    elif load_behavior == "current+history":
        if (history_table is None) or (history_table == ""):
            raise ValueError(
                "history_table should be provided when load_behavior is `current+history`."
            )
        conn.execute(sa_text(f"TRUNCATE TABLE {default_table}"))
        gdata.to_sql(
            default_table,
            conn,
            if_exists="append",
            index=False,
            schema="public",
            dtype={geometry_col: Geometry(geometry_type, srid=4326)},
        )
        gdata.to_sql(
            history_table,
            conn,
            if_exists="append",
            index=False,
            schema="public",
            dtype={geometry_col: Geometry(geometry_type, srid=4326)},
        )
    else:
        raise ValueError(
            "load_behavior should be one of `append`, `replace`, `current+history`."
        )
    conn.commit()
    conn.close()

    # print
    cost_time = time.time() - start_time
    print(f"GeoData been saved, cost time: {cost_time:.2f}s.")
