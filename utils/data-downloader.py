import asyncio
import aiohttp
import json
import gzip
import shutil
import time
import os
from io import BytesIO


S3_BUCKET_URL = "https://power-rankings-dataset-gprhack.s3.us-west-2.amazonaws.com"


async def download_gzip_and_write_to_json(session, file_name):
    # If file already exists locally do not re-download game
    if os.path.isfile(f"{file_name}.json"):
        return

    async with session.get(f"{S3_BUCKET_URL}/{file_name}.json.gz") as response:
        if response.status == 200:
            try:
                gzip_bytes = BytesIO(await response.read())
                with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                    with open(f"{file_name}.json", 'wb') as output_file:
                        shutil.copyfileobj(gzipped_file, output_file)
                print(f"{file_name}.json written")
            except Exception as e:
                print("Error:", e)
        else:
            print(f"Failed to download {file_name}")


async def download_esports_files(session):
    directory = "lol-esports-data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    esports_data_files = ["leagues", "tournaments", "players", "teams", "mapping_data"]
    tasks = [download_gzip_and_write_to_json(session, f"{directory}/{file_name}") for file_name in esports_data_files]
    await asyncio.gather(*tasks)


async def download_games(session, year):
    start_time = time.time()
    with open("esports-data/tournaments.json", "r") as json_file:
        tournaments_data = json.load(json_file)
    with open("esports-data/mapping_data.json", "r") as json_file:
        mappings_data = json.load(json_file)

    directory = "games"
    if not os.path.exists(directory):
        os.makedirs(directory)

    mappings = {
        esports_game["esportsGameId"]: esports_game for esports_game in mappings_data
    }

    game_counter = 0
    tasks = []

    for tournament in tournaments_data:
        start_date = tournament.get("startDate", "")
        if start_date.startswith(str(year)):
            print(f"Processing {tournament['slug']}")
            for stage in tournament["stages"]:
                for section in stage["sections"]:
                    for match in section["matches"]:
                        for game in match["games"]:
                            if game["state"] == "completed":
                                try:
                                    platform_game_id = mappings[game["id"]]["platformGameId"]
                                except KeyError:
                                    print(f"{platform_game_id} {game['id']} not found in the mapping table")
                                    continue

                                tasks.append(download_gzip_and_write_to_json(session, f"{directory}/{platform_game_id}"))
                                game_counter += 1

                            if game_counter % 10 == 0:
                                print(
                                    f"----- Processed {game_counter} games, current run time: \
                                    {round((time.time() - start_time)/60, 2)} minutes"
                                )
    await asyncio.gather(*tasks)


async def main():
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        await download_esports_files(session)
        await download_games(session, 2023)


if __name__ == "__main__":
    asyncio.run(main())