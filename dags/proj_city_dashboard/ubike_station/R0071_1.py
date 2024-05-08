import os
import sys

dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
sys.path.append(dags_path)
import pandas as pd
import requests
from settings.global_config import READY_DATA_DB_URI
from sqlalchemy import create_engine
from utils.auth_tdx import TDXAuth
from utils.load_stage import save_geodataframe_to_postgresql
from utils.transform_geometry import add_point_wkbgeometry_column_to_df
from utils.transform_time import convert_str_to_time_format

# Config
# Transactions across Taipei and New Taipei are frequently, so download both.
TPE_URL = r"https://tdx.transportdata.tw/api/basic/v2/Bike/Station/City/Taipei?%24format=JSON"
NTPE_URL = r"https://tdx.transportdata.tw/api/basic/v2/Bike/Station/City/NewTaipei?%24format=JSON"
FROM_CRS = 4326
LOAD_BEHAVIOR = "current+history"
DEFAULT_TABLE = "tran_ubike_station"
HISTORY_TABLE = "tran_ubike_station_history"
GEOMETRY_TYPE = "Point"

# Extract
# get token
tdx = TDXAuth()
token = tdx.get_token()
# get data
headers = {"authorization": f"Bearer {token}"}
tpe_response = requests.get(
    TPE_URL, headers=headers, timeout=60
)
if tpe_response.status_code != 200:
    raise ValueError(f"TPE request failed! Status: {tpe_response.status_code}")
ntpe_response = requests.get(
    NTPE_URL, headers=headers, timeout=60
)
if ntpe_response.status_code != 200:
    raise ValueError(f"NTPE request failed! Status: {ntpe_response.status_code}")

# Extract
# taipei
tpe_res_json = tpe_response.json()
tpe_data = pd.DataFrame(tpe_res_json)
tpe_data["county"] = "Taipei"
# new taipei
ntpe_res_json = ntpe_response.json()
ntpe_data = pd.DataFrame(ntpe_res_json)
ntpe_data["county"] = "New Taipei"
# merge
raw_data = pd.concat([tpe_data, ntpe_data])

# Transform
data = raw_data.copy()
# rename
col_map = {
    "StationUID": "station_uid",  # 唯一識別代碼，規則為 {業管機關代碼} + {StationID}
    "StationID": "station_id",
    "AuthorityID": "authority_id",
    "StationName": "name",
    "StationPosition": "pos",
    "StationAddress": "addr",
    "BikesCapacity": "bike_capacity",  # 可容納之自行車總數
    "ServiceType": "service_type",  # [1:'YouBike1.0',2:'YouBike2.0',3:'T-Bike',4:'P-Bike',5:'K-Bike']
    "SrcUpdateTime": "data_time",  # 來源端平台資料更新時間
    "UpdateTime": "tdx_update_time",  # TDX資料更新日期時間
    "county": "county",  # 縣市
}
data = data.rename(columns=col_map)
# extract nested json
data["name"] = data["name"].apply(lambda x: x["Zh_tw"])
data["name"] = data["name"].str.replace("YouBike2.0_", "")
data["addr"] = data["addr"].apply(lambda x: x["Zh_tw"])
data["lng"] = data["pos"].apply(lambda x: x["PositionLon"])
data["lat"] = data["pos"].apply(lambda x: x["PositionLat"])
data = data.drop(columns=["pos"])
# define column type
data["station_id"] = data["station_id"].astype(str)
data["bike_capacity"] = pd.to_numeric(data["bike_capacity"], errors="coerce")
# numbers can't be converted to int will be set to -1
data["bike_capacity"] = data["bike_capacity"].fillna(-1).astype(int)
# mapping category code to category name
data["service_type"] = data["service_type"].astype(str)
type_map = {
    "1": "UBike1.0",
    "2": "UBike2.0",
    "3": "TBike",
    "4": "PBike",
    "5": "KBike",
}
data["service_type"] = data["service_type"].map(type_map)
# time
data["data_time"] = convert_str_to_time_format(data["data_time"])
data["tdx_update_time"] = convert_str_to_time_format(data["tdx_update_time"])
# geometry
gdata = add_point_wkbgeometry_column_to_df(
    data, data["lng"], data["lat"], from_crs=FROM_CRS, is_add_xy_columns=False
)
# select column
ready_data = gdata[
    [
        "data_time",
        "county",
        "station_uid",
        "station_id",
        "authority_id",
        "name",
        "service_type",
        "bike_capacity",
        "addr",
        "lng",
        "lat",
        "wkb_geometry",
        "tdx_update_time",
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
