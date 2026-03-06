import requests
import datetime
import aiohttp
import asyncio

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


async def fetch_player_count(session, semaphore, api_key, appid, max_retries=5):
    params = {"key": api_key, "appid": appid}
    url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

    async with semaphore:
        for attempt in range(max_retries):
            await asyncio.sleep(0.1)

            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "appid": appid,
                        "player_count": data["response"].get("player_count", 0),
                        "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                elif response.status in [420, 429]:
                    print(f"Rate limited appid {appid}, attempt {attempt}")
                    await asyncio.sleep(10 * (attempt + 1))
                else:
                    # Genuine error (403, 404, etc.) — don't retry
                    return None

        # All retries exhausted — was rate limited
        return {"retry": True, "appid": appid}
    
async def get_all_player_counts(api_key, games):
    semaphore = asyncio.Semaphore(100)
    failed_games = []
    counter = {"done": 0}
    lock = asyncio.Lock()
    total = len(games)

    async def fetch_with_progress(session, game):
        result = await fetch_player_count(session, semaphore, api_key, game["appid"])
        async with lock:
            counter["done"] += 1
            if counter["done"] % 1000 == 0:
                print(f"Progress: {counter['done']}/{total}")
            if result and result.get("retry"):
                failed_games.append(game)
            return result

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_progress(session, game) for game in games]
        results = await asyncio.gather(*tasks)

    valid = [r for r in results if r is not None and not r.get("retry")]
    print(f"Got {len(valid)} results. Failed: {len(failed_games)}")

    # Retry failed games with lower concurrency
    retry_round = 1
    while failed_games and retry_round <= 3:
        await asyncio.sleep(10)
        print(f"Retry round {retry_round}: {len(failed_games)} games")
        retry_list = failed_games.copy()
        failed_games.clear()
        counter["done"] = 0

        async with aiohttp.ClientSession() as session:
            tasks = [fetch_with_progress(session, game) for game in retry_list]
            retry_results = await asyncio.gather(*tasks)

        valid.extend([r for r in retry_results if r is not None and not r.get("retry")])
        retry_round += 1

    print(f"Final: {len(valid)} results out of {total} games")
    return valid
