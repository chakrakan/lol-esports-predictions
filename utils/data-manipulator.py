import asyncio
import concurrent.futures
import gzip
import json
import os
from io import StringIO

import aiofiles
import pandas as pd

# Games manipulator
GAMES_DIR = "games"
TARGET_DIR = "games-parquet"
ESPORTS_DIR = "esports-data/lol-esports-data"


def convert_game_files_to_parquet():
    json_files = [f for f in os.listdir(GAMES_DIR) if f.endswith(".json")]
    for file in json_files:
        df = pd.read_json(file)
        target_file = os.path.join(TARGET_DIR, os.path.splitext(file)[0] + ".parquet")
        df.to_parquet(target_file, engine="pyarrow")
        print(f"Converted {file} to Parquet.")


def convert_data_files_to_parquet():
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
    for file_name in esports_data_files:
        df = pd.read_json(f"{ESPORTS_DIR}/{file_name}.json")
        target_file = os.path.join(ESPORTS_DIR, os.path.splitext(file_name)[0] + ".parquet")
        df.to_parquet(target_file, engine="pyarrow")
        print(f"Converted {file_name} to Parquet.")


def gzip_to_json(in_file, out_file):
    with gzip.open(in_file, "rt", encoding="utf-8") as file:
        data = json.load(file)
    with open(out_file, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def save_as_json(data, output_file_path):
    with open(output_file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def convert_parquet_to_csv(parquet_file_path, csv_file_path):
    df = pd.read_parquet(parquet_file_path)
    df.to_csv(csv_file_path, index=False)


def convert_json_to_csv(json_file_path, csv_file_path):
    df = pd.read_json(json_file_path)
    df.to_csv(csv_file_path, index=False)


async def convert_csv_to_parquet(csv_file, parquet_file):
    async with aiofiles.open(csv_file, mode="r") as f:
        content = await f.read()
        df = pd.read_csv(StringIO(content))
        df.to_parquet(parquet_file)


def main():
    gzip_to_json("games/ESPORTSTMNT03 3195276.json.gz", "games/ESPORTSTMNT03:3195276.json")
    # Get list of JSON files
    # json_files = [f for f in os.listdir(GAMES_DIR) if f.endswith('.json')]

    # # Convert files concurrently
    # with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
    #     executor.map(convert_to_parquet, json_files)


# async def main():
# files_csv = [
#     (f'{ESPORTS_DATA_DIR}/tournament-game-data.csv', f'{ESPORTS_DATA_DIR}/tournament-game-data.parquet'),
# ]

# files_json = [
#     (f'{ESPORTS_DATA_DIR}/tournaments.json', f'{ESPORTS_DATA_DIR}/tournaments.parquet'),
# ]

# tasks_csv = [convert_csv_to_parquet(csv_file, parquet_file) for csv_file, parquet_file in files_csv]
# tasks_json = [convert_json_to_parquet(json_file, parquet_file) for json_file, parquet_file in files_json]
# await asyncio.gather(*tasks_csv)
# await asyncio.gather(*tasks_json)

if __name__ == "__main__":
    # asyncio.run(main())
    main()
