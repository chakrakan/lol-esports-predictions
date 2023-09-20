import gzip
from io import StringIO
import json
import asyncio
import aiofiles
import pandas as pd
import os
import concurrent.futures


ESPORTS_DATA_DIR = "esports-data"

# Games manipulator
GAMES_DIR = "games"
TARGET_DIR = "games-parquet"

def convert_to_parquet(json_file):
    source_file = os.path.join(GAMES_DIR, json_file)
    target_file = os.path.join(TARGET_DIR, os.path.splitext(json_file)[0] + '.parquet')

    df = pd.read_json(source_file)
    df.to_parquet(target_file, engine='pyarrow')

    print(f"Converted {json_file} to Parquet.")


def read_gzipped_json(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as file:
        data = json.load(file)
    return data

def save_as_json(data, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def convert_parquet_to_csv(parquet_file_path, csv_file_path):
    df = pd.read_parquet(parquet_file_path)
    df.to_csv(csv_file_path, index=False)

async def convert_csv_to_parquet(csv_file, parquet_file):
    async with aiofiles.open(csv_file, mode='r') as f:
        content = await f.read()
        df = pd.read_csv(StringIO(content))
        df.to_parquet(parquet_file)

def main():
    convert_parquet_to_csv(f"{ESPORTS_DATA_DIR}/cleaned_distinct_team_player_mapping.parquet", f"{ESPORTS_DATA_DIR}/cleaned_distinct_team_player_mapping.csv")
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