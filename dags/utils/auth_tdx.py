import pickle
from datetime import datetime, timedelta

import requests
from settings.global_config import DATA_PATH, PROXIES

TOKEN_URL = (
    "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
)
HEADERS = {"content-type": "application/x-www-form-urlencoded"}
FILE_NAME = "tdx_token.pickle"


class TDXAuth:
    """
    The class for authenticating with the 運輸資料流通服務平臺(Transport Data eXchange , TDX) API.
    The class loads the client ID and client secret from the Airflow variables.
    The access token is saved to a file for reuse.
    
    Example:
        ``` python
        # Get Taipei YouBike stations from TDX
        import os
        import sys

        dags_path = os.path.join(os.getcwd(), 'dags')  # Should be looks like '.../dags'
        sys.path.append(dags_path)
        import requests
        import pandas as pd
        from utils.auth_tdx import TDXAuth

        TPE_URL = r"https://tdx.transportdata.tw/api/basic/v2/Bike/Station/City/Taipei?%24format=JSON"

        tdx = TDXAuth()
        token = tdx.get_token()
        # get data
        headers = {"authorization": f"Bearer {token}"}
        tpe_response = requests.get(
            TPE_URL, headers=headers, timeout=60
        )
        tpe_res_json = tpe_response.json()
        tpe_data = pd.DataFrame(tpe_res_json)
        print(tpe_data)
        ```
        ```
        >>> print(tpe_data.iloc[0]) 
        StationUID                                              TPE500101001
        StationID                                                  500101001
        AuthorityID                                                      TPE
        StationName        {'Zh_tw': 'YouBike2.0_捷運科技大樓站', 'En': 'YouBike...
        StationPosition    {'PositionLon': 121.5436, 'PositionLat': 25.02...
        StationAddress     {'Zh_tw': '復興南路二段235號前', 'En': 'No.235, Sec. 2...
        BikesCapacity                                                     28
        ServiceType                                                        2
        SrcUpdateTime                              2024-05-06T20:43:18+08:00
        UpdateTime                                 2024-05-06T20:43:26+08:00
        Name: 0, dtype: object
        ```
    """

    def __init__(self):
        self.client_id = "tuic-52d7ffb2-4912-46ba"
        self.client_secret = "6e262b4b-ef32-4d4f-b067-f1984077463e"
        self.full_file_path = f"{DATA_PATH}/{FILE_NAME}"

    def get_token(self, is_proxy=True, timeout=60):
        """
        Get the access token for authentication.
        This method retrieves the access token from the specified path.
        If the token is not found or has expired, a new token is obtained and saved to the path.

        Args:
            is_proxy (bool): Flag indicating whether to use a proxy. Defaults to True.
            timeout (int): The timeout for the request. Defaults to 60.

        Returns:
            str: The access token.

        Raises:
            FileNotFoundError: If the token file is not found.
            EOFError: If the token file is empty or corrupted.
        """
        # check if the token is expired
        now_time = datetime.now()
        try:
            with open(self.full_file_path, "rb") as handle:
                res = pickle.load(handle)
                expired_time = res["expired_time"]
                if now_time < expired_time:  # If the token is not expired
                    return res["access_token"]
        except (FileNotFoundError, EOFError):
            pass

        # get the token
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        with requests.post(
            TOKEN_URL,
            headers=HEADERS,
            data=data,
            proxies=PROXIES if is_proxy else None,
            timeout=timeout,
        ) as response:
            res_json = response.json()
            token = res_json["access_token"]
            expired_time = now_time + timedelta(seconds=res_json["expires_in"])
            res = {"access_token": token, "expired_time": expired_time}

        # save the token
        with open(self.full_file_path, "wb") as handle:
            pickle.dump(res, handle)

        return token
