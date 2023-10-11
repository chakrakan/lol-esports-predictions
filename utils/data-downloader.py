import asyncio
import csv
import gzip
import json
import os
import shutil
import time
from io import BytesIO
from typing import Optional

import aiohttp

S3_BUCKET_URL = "https://power-rankings-dataset-gprhack.s3.us-west-2.amazonaws.com"


async def download_gzip_and_write_to_json(session, remote_file_name: str, local_file_name: Optional[str] = None):
    # If file already exists locally do not re-download game
    if os.path.isfile(f"{local_file_name}.json"):
        return

    async with session.get(f"{S3_BUCKET_URL}/{remote_file_name}.json.gz") as response:
        print(response)
        if response.status == 200:
            try:
                gzip_bytes = BytesIO(await response.read())
                with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                    with open(f"{local_file_name}.json", "wb") as output_file:
                        shutil.copyfileobj(gzipped_file, output_file)
                print(f"{local_file_name}.json written")
            except Exception as e:
                print("Error:", e)
        else:
            print(f"Failed to download {remote_file_name}")


async def download_esports_files(session):
    local_directory = "lol-esports-data"
    remote_directory = "esports-data"
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    esports_data_files = [
        "leagues",
        "tournaments",
        "players",
        "teams",
        "mapping_data",
        "tournaments_without_game_data",
        "unfiltered_players",
        "unfiltered_teams",
    ]
    tasks = [
        download_gzip_and_write_to_json(session, f"{remote_directory}/{file_name}", f"{local_directory}/{file_name}")
        for file_name in esports_data_files
    ]
    await asyncio.gather(*tasks)


async def download_games(session, year: int, num_games: Optional[int] = None):
    start_time = time.time()

    directory = "games"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # use games that have completed and have an actual winner
    # filter by year, for 2023, len = 7855
    with open("esports-data/tournament-game-data.csv", "r") as csv_file:
        reader = csv.DictReader(csv_file)
        completed_game_ids = [row["game_id"] for row in reader if row.get("startDate", "").startswith(str(year))]

    print(f"Completed Game IDs: {len(completed_game_ids)}")
    mappings = {}

    # use mapping-data with cleaned up rows where we don't have info
    # for 2023, len = 503
    with open("esports-data/filtered-mapping-data.csv", "r") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row["esportsGameId"] in completed_game_ids:
                mappings[row["esportsGameId"]] = row["platformGameId"]

    game_counter = 0
    total_games = len(mappings)
    tasks = []

    for platform_id in mappings.values():
        tasks.append(
            download_gzip_and_write_to_json(session, f"{directory}/{platform_id}", f"{directory}/{platform_id}")
        )
        game_counter += 1

        if game_counter % 10 == 0:
            await asyncio.gather(*tasks)
            tasks = []  # Resetting tasks list after awaiting
            print(
                f"----- Processed {game_counter} games/{total_games}, current run time: \
                {round((time.time() - start_time)/60, 2)} minutes"
            )
        if game_counter == num_games:
            print(
                f"----- Processed {game_counter} games, total run time: \
            {round((time.time() - start_time)/60, 2)} minutes"
            )
            print("----- Downloading completed")
            print("----- Exiting...")
            return

    if tasks:
        await asyncio.gather(*tasks)


async def main():
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        await download_esports_files(session)
        # await download_games(session, 2023)


if __name__ == "__main__":
    asyncio.run(main())
