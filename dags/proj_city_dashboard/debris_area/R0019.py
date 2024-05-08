import os
import sys

sys.path.append(os.path.join(os.getcwd(), "dags"))

import geopandas as gpd
from settings.global_config import DATA_PATH, READY_DATA_DB_URI
from sqlalchemy import create_engine
from utils.extract_stage import download_file, unzip_file_to_target_folder
from utils.load_stage import save_geodataframe_to_postgresql
from utils.transform_geometry import (
    convert_geometry_to_wkbgeometry,
    convert_polygon_to_multipolygon,
)

# Config
URL = "https://data.moa.gov.tw/OpenData/GetOpenDataFile.aspx?id=I89&FileType=SHP&RID=27238"
FILE_NAME = "debris_area.zip"
unzip_path = f"{DATA_PATH}/debris_area"
ENCODING = "UTF-8"
FROM_CRS = 3826
LOAD_BEHAVIOR = "current+history"
DEFAULT_TABLE = "patrol_debrisarea"
HISTORY_TABLE = "patrol_debrisarea_history"
GEOMETRY_TYPE = "MultiPolygon"

# Extract shpfile
zip_file = download_file(FILE_NAME, URL)
unzip_file_to_target_folder(zip_file, unzip_path)
target_shp_file = [f for f in os.listdir(unzip_path) if f.endswith("shp")][0]
raw_data = gpd.read_file(
    f"{unzip_path}/{target_shp_file}", encoding=ENCODING, from_crs=FROM_CRS
)

# Transform
gdata = raw_data.copy()
# rename
gdata.columns = gdata.columns.str.lower()
gdata = gdata.rename(
    columns={
        "id": "id",
        "debrisno": "debrisno",  # 土石流潛勢溪流編號
        "county": "county",  # 縣市
        "town": "town",  # 鄉鎮市區
        "vill": "vill",  # 村里
        "overflowno": "overflowno",  # 溢流點編號
        "overflow_x": "overflow_x",
        "overflow_y": "overflow_y",
        "address": "address",  # 保全住戶地址
        "total_res": "total_res",  # 影響範圍內保全住戶總數
        "res_class": "res_class",  # 影響範圍內保全住戶戶數級距
        "risk": "risk",  # 風險等級
        "dbno_old": "dbno_old",  # 土石流潛勢溪流前次編號
    }
)
# geometry
# there some polygon and multipolygon in geometry column, convert them all to multipolygon
gdata["geometry"] = gdata["geometry"].apply(convert_polygon_to_multipolygon)
gdata = convert_geometry_to_wkbgeometry(gdata, from_crs=FROM_CRS)
# secelt columns
ready_data = gdata[
    [
        "id",
        "debrisno",
        "county",
        "town",
        "vill",
        "overflowno",
        "overflow_x",
        "overflow_y",
        "address",
        "total_res",
        "res_class",
        "risk",
        "dbno_old",
        "wkb_geometry",
    ]
]

# Load
engine = create_engine(READY_DATA_DB_URI)
save_geodataframe_to_postgresql(
    engine,
    gdata=ready_data,
    load_behavior=LOAD_BEHAVIOR,
    default_table=DEFAULT_TABLE,
    history_table=HISTORY_TABLE,
    geometry_type=GEOMETRY_TYPE,
)
