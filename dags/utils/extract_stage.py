import json
import zipfile
from pathlib import Path

import fiona
import geopandas as gpd
import requests
from settings.global_config import DATA_PATH, PROXIES


def download_file(
    file_name,
    url,
    is_proxy=False,
    is_verify=True,
    timeout: int = 60,
    file_folder=DATA_PATH,
):
    """
    Download file from `url` to `{DATA_PATH}/{file_name}`.

    Args:
        file_name: str, file name
        url: str, file url
        is_proxy: bool, whether use proxy
        is_verify: bool, whether verify ssl
        timeout: int, request timeout
        file_folder: str, file folder path

    Returns: str, full file path

    Example:
        ``` python
        # Read GeoJSON
        # GeoJSON is a special format of JSON that represents geographical data
        # The extension of a geoJSON file can be .geojson or .json.
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import geopandas as gpd
        from utils.extract_stage import download_file

        URL = "https://pwdgis.taipei/wg/opendata/I0201-5.geojson"
        FILE_NAME = "goose_sanctuary.geojson"
        FILE_ENCODING = "UTF-8"

        local_file = download_file(FILE_NAME, URL)
        gdata = gpd.read_file(local_file, encoding=FILE_ENCODING, driver="GeoJSON")
        print(gdata)
        ```
        ```
        >>> output:
        Id     名稱            面積    類型  集水區  物理型  水文HY  濱水植  水質WQ  生物BI  MIWC2017                                           geometry
        0   3  雁鴨保護區  1.799444e+06  重要濕地  NaN  NaN   NaN  NaN   NaN   NaN       NaN  MULTIPOLYGON (((121.51075 25.02214, 121.51083 ...
        ```
    """
    full_file_path = f"{file_folder}/{file_name}"
    # download file
    try:
        with requests.get(
            url,
            stream=True,
            proxies=PROXIES if is_proxy else None,
            verify=is_verify,
            timeout=timeout,
        ) as r:
            r.raise_for_status()
            with open(full_file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {file_name} from {url}")
        return full_file_path
    except Exception as e:
        raise e


def unzip_file_to_target_folder(zip_file: str, unzip_path: str):
    """
    Unzip .zip file from `zip_file` to `target_folder`.

    Args:
        zip_file: str, zip file path
        target_folder: str, target folder path

    Returns: None

    Example:
        ``` python
        # Read Shapefile
        # Shapefile is a popular geospatial vector data format for geographic information system software.
        # The shapefile is a set of files with the same name but different extensions.
        # Usually, theses files been compressed into a zip file.
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import geopandas as gpd
        from utils.extract_stage import (
            download_file,
            unzip_file_to_target_folder,
        )
        from settings.global_config import DATA_PATH

        URL = r"https://data.moa.gov.tw/OpenData/GetOpenDataFile.aspx?id=I88&FileType=SHP&RID=27237"
        FILE_NAME = "debris_area.zip"
        unzip_path = f"{DATA_PATH}/debris_area"
        FILE_ENCODING = "UTF-8"

        zip_file = download_file(FILE_NAME, URL)
        unzip_file_to_target_folder(zip_file, unzip_path)
        target_shp_file = [f for f in os.listdir(unzip_path) if f.endswith("shp")][0]
        gdata = gpd.read_file(f"{unzip_path}/{target_shp_file}", encoding=FILE_ENCODING)
        ```
        ```
        >>> output:
                ID Debrisno  ... Dbno_old                                           geometry
        0        1  宜縣DF135  ...   宜蘭A089  LINESTRING (313537.820 2726900.950, 313625.420...
        1        2  宜縣DF131  ...   宜蘭A088  LINESTRING (319284.480 2727626.340, 319308.250...
        2        3  宜縣DF132  ...   宜蘭A087  LINESTRING (318877.260 2727421.020, 318878.620...
        3        4  宜縣DF133  ...   宜蘭A086  MULTILINESTRING ((317842.890 2725794.540, 3178...
        4        5  宜縣DF134  ...    宜蘭028  MULTILINESTRING ((315765.720 2726200.720, 3157...
        ...    ...      ...  ...      ...                                                ...
        1727  1728  花縣DF098  ...    花蓮020  LINESTRING (303782.140 2619541.820, 303857.320...
        1728  1729  花縣DF103  ...   花蓮A138  LINESTRING (302751.200 2607101.490, 302746.680...
        1729  1730  花縣DF104  ...   花蓮A139  LINESTRING (302677.050 2606792.820, 302667.830...
        1730  1731  花縣DF105  ...    花蓮025  MULTILINESTRING ((300594.180 2604587.920, 3005...
        1731  1732  花縣DF106  ...    花蓮026  MULTILINESTRING ((300470.400 2604218.870, 3004...

        [1732 rows x 31 columns]
        ```
    """
    # Create unzip destination folder
    unzip_dir = Path(unzip_path)
    if not unzip_dir.exists():
        unzip_dir.mkdir()

    # Unzip file
    with zipfile.ZipFile(zip_file) as z:
        z.extractall(unzip_dir)

    print(f"Unzip {zip_file} to {unzip_path}")


def read_kml(file):
    """
    Read kml file to geopandas dataframe.
    Note:
    Read kmz file is similar to read kml file. Kmz is a zip file including kml file.
    The steps to get kml file from kmz file:
    1. rename file extension from `{file_name}.kmz` to `{file_name}.zip`
    2. unzip `{file_name}.zip`
    3. read file `doc.kml` in the unzipped folder

    Args:
        file: str, kml file path

    Returns: geopandas dataframe

    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        from utils.extract_stage import download_file, read_kml

        URL = "https://data.taipei/api/frontstage/tpeod/dataset/resource.download?rid=a69988de-6a49-4956-9220-40ebd7c42800"
        FILE_NAME = "urban_bike_path.kml"

        res = download_file(FILE_NAME, URL)
        df = read_kml(res)
        print(df.iloc[0])
        ```
        ```
        >>> print(df.iloc[0])
        Name                                                    三元街(西南側)
        Description      編號：TP2329 名稱：三元街(西南側) 縣市別：台北市 起點描述：南海路 迄點描述：泉州街
        geometry       LINESTRING Z (121.514241 25.027622 0, 121.5133...
        Name: 0, dtype: object
        ```
    """
    fiona.drvsupport.supported_drivers["KML"] = "rw"
    df = gpd.read_file(file, driver="KML")
    return df


def get_data_taipei_api(rid, timeout=60):
    """
    Retrieve data from Data.taipei API by automatically traversing all data.
    (The Data.taipei API returns a maximum of 1000 records per request, so offset is used to
    obtain all data.)

    Args:
        rid (str): The resource ID of the dataset.
        timeout (int, optional): The timeout limit for the HTTP request in seconds. Defaults to 60.

    Returns:
        list: A list containing all data retrieved from the Data.taipei API.

    Example:
        ``` python
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import pandas as pd
        from utils.extract_stage import get_data_taipei_api

        rid = "04a3d195-ee97-467a-b066-e471ff99d15d"
        res = get_data_taipei_api(rid)
        df = pd.DataFrame(res)
        print(df.iloc[0])
        ```
        ```
        >>> print(df.iloc[0])
        {'_id': 1, '_importdate': {'date': '2024-03-01 14:46:51.602832', 'timezone_type': 3, 'timezone': 'Asia/Taipei'}, '機構名稱': '郵政醫院（委託中英醫療社團法人經營）', '地址': '臺北市中正區福州街14號', 'x': '121.5186982', 'y': '25.02874869'}
        ```
    """
    url = f"https://data.taipei/api/v1/dataset/{rid}?scope=resourceAquire"
    response = requests.get(url, timeout=timeout)
    data_dict = response.json()
    count = data_dict["result"]["count"]
    res = []
    offset_count = int(count / 1000)
    for i in range(offset_count + 1):
        i = i * 1000
        url = f"https://data.taipei/api/v1/dataset/{rid}?scope=resourceAquire&offset={i}&limit=1000"
        response = requests.get(url, timeout=timeout)
        get_json = response.json()
        res.extend(get_json["result"]["results"])
    return res


def get_data_taipei_file_last_modified_time(page_id, rank=0, timeout=30):
    """
    Request the file update time of given data.taipei page_id.
    file_last_modified is usually located at the end of the page, next to the download button.
    If a page has more than one file, you can specify the rank.

    Args:
        url (str): The URL of the data.taipei resource.
        rank (int, optional): The rank of the file last modified record. Defaults to 0 is top one.
        timeout (int, optional): The timeout limit for the HTTP request in seconds. Defaults to 30.

    Returns:
        str: The last modified time of the given data.taipei resource.

    Example:
        ``` python
        from utils.extract_stage import get_data_taipei_file_last_modified_time

        PAGE_ID = "4fefd1b3-58b9-4dab-af00-724c715b0c58"

        res = get_data_taipei_file_last_modified_time(PAGE_ID)
        print(res)
        ```
        ```
        >>> print(res)
        '2023-06-06 09:53:08'
        ```
    """
    url = f"https://data.taipei/api/frontstage/tpeod/dataset.view?id={page_id}"
    res = requests.get(url, timeout=timeout)
    if res.status_code != 200:
        raise ValueError(f"Request Error: {res.status_code}")

    data_info = json.loads(res.text)
    lastest_modeified_time = data_info["payload"]["resources"][rank]["last_modified"]
    return lastest_modeified_time


def get_data_taipei_page_change_time(page_id, rank=0, timeout=30):
    """
    Request the page change time of given data.taipei page_id.
    page_change_time is located at the left side tab 異動紀錄.
    The page_change_time usually have more than one record, so you can specify the rank.

    Args:
        url (str): The URL of the data.taipei resource.
        rank (int, optional): The rank of the page change time record. Defaults to 0 is lastest.
        timeout (int, optional): The timeout limit for the HTTP request in seconds. Defaults to 30.

    Returns:
        str: The page change time of the given data.taipei resource.

    Example:
        ``` python
        from utils.extract_stage import get_data_taipei_page_change_time

        PAGE_ID = "4fefd1b3-58b9-4dab-af00-724c715b0c58"

        res = get_data_taipei_page_change_time(PAGE_ID)
        print(res)
        ```
        ```
        >>> print(res)
        2023-09-08 10:02:06
        ```
    """
    url = f"https://data.taipei/api/frontstage/tpeod/dataset/change-history.list?id={page_id}"
    res = requests.get(url, timeout=timeout)
    if res.status_code != 200:
        raise ValueError(f"Request Error: {res.status_code}")

    update_history = json.loads(res.text)
    lastest_update = update_history["payload"][rank]
    lastest_update_time = lastest_update.split("更新於")[-1]
    return lastest_update_time.strip()
