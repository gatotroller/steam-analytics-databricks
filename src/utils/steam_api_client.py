import requests

def get_app_list(steam_key):

    #URLS
    STEAM_GetAppList_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
    have_more = True
    apps = []
    last_appid = None
    params={
        "key": steam_key,
        "max_results": 50000,
        "include_games": True,
        "include_dlc": False,
        "include_software": False,
        "include_videos": False,
        "include_hardware": False,
    }

    while have_more:

        if last_appid:
            params["last_appid"] = last_appid

        response = requests.get(
            url=STEAM_GetAppList_URL,
            params=params
        )

        status_code = response.status_code
        data = response.json()

        if status_code != 200:
            raise Exception(f"API call failed with status {status_code}")

        apps.extend(data["response"]["apps"])
        have_more = data["response"].get("have_more_results", False)
        last_appid = data["response"].get("last_appid")

    return apps


