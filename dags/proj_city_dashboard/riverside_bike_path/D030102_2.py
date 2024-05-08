import os
import sys

sys.path.append(os.path.join(os.getcwd(), "dags"))
import xml.etree.ElementTree as ET

import geopandas as gpd
import pandas as pd
from settings.global_config import READY_DATA_DB_URI
from shapely.geometry import LineString
from sqlalchemy import create_engine
from utils.extract_stage import (
    download_file,
    get_data_taipei_file_last_modified_time,
    read_kml
)
from utils.load_stage import save_geodataframe_to_postgresql
from utils.transform_geometry import (
    convert_geometry_to_wkbgeometry,
    convert_linestring_to_multilinestring
)
from utils.transform_time import convert_str_to_time_format

# Config
LOAD_BEHAVIOR = "current+history"
DEFAULT_TABLE = "work_riverside_bike_path"
HISTORY_TABLE = "work_riverside_bike_path_history"
URL = "https://data.taipei/api/frontstage/tpeod/dataset/resource.download?rid=0912d803-688f-493a-b682-27da729ed593"
PAGE_ID = "4fefd1b3-58b9-4dab-af00-724c715b0c58"
GEOMETRY_TYPE = "MultiLineStringZ"
FILE_NAME = "riverside_bike_path.kml"
FROM_CRS = 4326

# Extract
# get xml tree
local_file = download_file(FILE_NAME, URL)
raw_data = read_kml(local_file)
raw_data['data_time'] = get_data_taipei_file_last_modified_time(PAGE_ID)

# Transform
gdata = raw_data.copy()
# rename
gdata.columns = gdata.columns.str.lower()
# extract columns
gdata['item'] = gdata['description'].str.split('fid:').str[1].str.split('<br>').str[0]
gdata['route'] = ""
gdata['length'] = gdata['description'].str.split('length\\(M\\):', ).str[1].str.split('<br>').str[0]
gdata["cost_time"] = gdata["description"].str.extract("（約(.*?)分鐘）")
# define column type
gdata["cost_time"] = pd.to_numeric(gdata["cost_time"], errors="coerce")
gdata["length"] = pd.to_numeric(gdata["length"].str.strip(), errors="coerce")
gdata["item"] = gdata['item'].str.strip().astype(int)
# time
gdata["data_time"] = convert_str_to_time_format(gdata["data_time"])
# geometry
gdata["geometry"] = gdata["geometry"].apply(convert_linestring_to_multilinestring)
gdata = convert_geometry_to_wkbgeometry(gdata, from_crs=FROM_CRS)
# select column
ready_data = gdata[
    [
        "data_time",
        "item",
        "route",
        "name",
        "length",
        "cost_time",
        "description",
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
