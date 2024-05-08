import os
import sys

dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
sys.path.append(dags_path)
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from settings.global_config import DATA_PATH, READY_DATA_DB_URI
from sqlalchemy import create_engine
from utils.extract_stage import get_data_taipei_file_last_modified_time
from utils.load_stage import save_dataframe_to_postgresql
from utils.transform_time import convert_str_to_time_format

# Config
URL = "https://data.taipei/api/frontstage/tpeod/dataset/resource.download?rid=43624c8e-c768-4b3c-93c4-595f5af7a9cb"
file_path = f"{DATA_PATH}/permit.xml"
PAGE_ID = "d8834353-ff8e-4a6c-9730-a4d3541f2669"
FROM_CRS = 4326
LOAD_BEHAVIOR = "replace"
DEFAULT_TABLE = "building_permit"
HISTORY_TABLE = "building_permit_history"
GEOMETRY_TYPE = "MultiPolygon"

# Extract
# download XML
res = requests.get(URL, timeout=300)
if res.status_code != 200:
    raise ValueError(f"Request Error: {res.status_code}")

with open(file_path, "wb") as f:
    f.write(res.content)
# parse xml
tree = ET.parse(file_path)
root = tree.getroot()
temps = []
for permit in root:
    temp = {}
    # A permit could have multiple land units.
    # For our purpose, we divide each land unit into a row.
    number_of_land_units = len(permit.find("地段地號"))
    for index_of_land_unit in range(number_of_land_units):
        # dynamic construct each column
        for col in permit:
            if col.tag == "地段地號":
                temp[col.tag] = col[index_of_land_unit].text
            if col.tag == "建築地點":
                # dashboard show a point for a permit, so only need to extract the first building address.
                temp[col.tag] = col[0].text if len(col) > 0 else None
            elif col.tag in ["建物資訊", "建物面積", "地段地號"]:
                # some info have sub item and not a fixed length, so need to loop to extract.
                for sub_info in col:
                    temp[sub_info.tag] = sub_info.text
            else:
                temp[col.tag] = col.text
        temps.append(temp)
raw_data = pd.DataFrame(temps)
# add updata time
raw_data["data_time"] = get_data_taipei_file_last_modified_time(PAGE_ID)

# Transform
data = raw_data.copy()
# standardize time
data["data_time"] = convert_str_to_time_format(data["data_time"])
data["發照日期"] = convert_str_to_time_format(data["發照日期"], from_format="%TY%m%d")
data["epoch_time"] = data["發照日期"].map(lambda x: x.timestamp())
ready_data = data

# Load
engine = create_engine(READY_DATA_DB_URI)
save_dataframe_to_postgresql(
    engine,
    data=ready_data,
    load_behavior=LOAD_BEHAVIOR,
    default_table=DEFAULT_TABLE
)
