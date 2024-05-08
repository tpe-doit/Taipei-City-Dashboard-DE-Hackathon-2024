import os
import sys

sys.path.append(os.path.join(os.getcwd(), "dags"))
from datetime import datetime

import geopandas as gpd
import pytz
from settings.global_config import READY_DATA_DB_URI
from sqlalchemy import create_engine
from utils.extract_stage import download_file
from utils.load_stage import save_geodataframe_to_postgresql
from utils.transform_geometry import convert_geometry_to_wkbgeometry
from utils.transform_time import convert_str_to_time_format

# Config
TAIPEI_TZ = pytz.timezone("Asia/Taipei")
URL = "https://tpnco.blob.core.windows.net/blobfs/Data/TP_SIDEWORK.json"
FILE_NAME = "sidewalk.json"
ENCODING = "UTF-8"
FROM_CRS = 3826
LOAD_BEHAVIOR = "current+history"
DEFAULT_TABLE = "work_sidewalk"
HISTORY_TABLE = "work_sidewalk_history"
GEOMETRY_TYPE = "MultiPolygon"

# Extract
local_file = download_file(FILE_NAME, URL, timeout=300)
raw = gpd.read_file(local_file, encoding=ENCODING)

# Transform
gdata = raw.copy()
# define column type
gdata.columns = gdata.columns.str.lower()
gdata["objectid"] = gdata["objectid"].astype(str)
gdata["data_time"] = datetime.now(tz=TAIPEI_TZ).replace(microsecond=0)
gdata["data_time"] = convert_str_to_time_format(gdata["data_time"])
# geometry
gdata = convert_geometry_to_wkbgeometry(gdata, from_crs=FROM_CRS)
# reshape
col_map = {
    "objectid": "id",
    "town_n": "dist",
    "name_road": "name",  # 人行道名稱
    "pstart": "start_road",  # 起始路名
    "pend": "end_road",  # 結束路名
    "sw_direct": "direction",  # 街道的哪一側
    "sw_leng_itemvalue": "length",
    "sw_wth_itemvalue": "width",
    "shape_ar_itemvalue": "area",
    "sww_wth": "clear_width",  # 淨寬
    "geometry": "geometry",
}
gdata = gdata.rename(columns=col_map)
# select column
ready_data = gdata[
    [
        "data_time",
        "id",
        "dist",
        "name",
        "start_road",
        "end_road",
        "length",
        "width",
        "area",
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
