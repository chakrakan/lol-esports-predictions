import csv
import gzip
import json
import logging
import os
import shutil
from datetime import datetime
from io import BytesIO
from typing import Dict, Optional, Union

import pandas as pd

# Logging configuration
logging.basicConfig(level=logging.INFO)

import requests
from constants import (
    BUILDING_DESTROYED,
    CHAMPION_KILL,
    CREATED_DATA_DIR,
    DRAGON_TYPE_MAPPINGS,
    EPIC_MONSTER_KILL,
    GAMES_DIR,
    LANE_MAPPING,
    LOL_ESPORTS_DATA_DIR,
    ROLES,
    S3_BUCKET_URL,
    STR_SIDE_MAPPING,
    TEAM_ID_TO_INFO_MAPPING_PATH,
    TIME_FORMAT,
    TOURNAMENT_TO_SLUGS_MAPPING_PATH,
    TURRET,
    Monsters,
    Turret,
)


def get_tournament_to_stage_slug_mapping():
    """
    Returns a dictionary mapping tournament slugs to stage slugs.
    """
    # if file exists read json from it
    if os.path.exists(TOURNAMENT_TO_SLUGS_MAPPING_PATH):
        with open(TOURNAMENT_TO_SLUGS_MAPPING_PATH, "r") as f:
            return json.load(f)
    else:
        try:
            with open(f"{LOL_ESPORTS_DATA_DIR}/tournaments.json", "r") as file:
                tournaments_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error reading tournaments.json: {e}")
            return {}

        tournament_stage_slugs_mapping = {}
        for tournament in tournaments_data:
            tournament_id = tournament.get("id")
            stage_slugs = [stage.get("slug") for stage in tournament.get("stages", []) if stage.get("slug")]
            if tournament_id and stage_slugs:
                tournament_stage_slugs_mapping[tournament_id] = stage_slugs

        try:
            with open(TOURNAMENT_TO_SLUGS_MAPPING_PATH, "w", encoding="utf-8") as f:
                json.dump(tournament_stage_slugs_mapping, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logging.error(f"Error writing to {TOURNAMENT_TO_SLUGS_MAPPING_PATH}: {e}")

        return tournament_stage_slugs_mapping


def get_team_id_to_info_mapping():
    """
    Returns a dictionary mapping team IDs to team info dictionaries.
    """
    if os.path.exists(TEAM_ID_TO_INFO_MAPPING_PATH):
        with open(TEAM_ID_TO_INFO_MAPPING_PATH, "r") as f:
            return json.load(f)
    else:
        try:
            with open(f"{LOL_ESPORTS_DATA_DIR}/teams.json", "r") as f:
                team_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error reading teams.json: {e}")
            return {}

        team_mapping = {
            team["team_id"]: {"team_name": team["name"], "team_code": team["acronym"]} for team in team_data
        }

        try:
            with open(TEAM_ID_TO_INFO_MAPPING_PATH, "w", encoding="utf-8") as f:
                json.dump(team_mapping, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logging.error(f"Error writing to {TEAM_ID_TO_INFO_MAPPING_PATH}: {e}")

        return team_mapping


def get_game_data(platform_game_id: str):
    """A modified version of the method provided by Riot/AWS for downloading game data.
    Reads from local if available, else downloads, writes to file, and returns json.

    Args:
        file_name (str): game file name
    """
    if os.path.exists(f"{GAMES_DIR}/{platform_game_id}.json"):
        with open(f"{GAMES_DIR}/{platform_game_id}.json", "r") as f:
            json_data = json.load(f)
            return json_data
    else:
        response = requests.get(f"{S3_BUCKET_URL}/{platform_game_id}.json.gz")
        if response.status_code == 200:
            try:
                gzip_bytes = BytesIO(response.content)
                with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                    with open(f"{GAMES_DIR}/{platform_game_id}.json", "wb") as output_file:
                        shutil.copyfileobj(gzipped_file, output_file)
                    print(f"{platform_game_id}.json written")
                    gzipped_content = gzipped_file.read().decode("utf-8")
                    json_data = json.loads(gzipped_content)
                    return json_data
            except Exception as e:
                print("Error:", e)
        else:
            print(f"Failed to download {platform_game_id}")


def non_empty_equal(str1: str, str2: str):
    """
    Check if both strings are non-empty and equal.

    Parameters:
    - str1, str2: Strings to compare.

    Returns:
    Boolean. True if both strings are non-empty and equal, False otherwise.
    """
    return bool(str1) and bool(str2) and str1 == str2


def get_team_side_data(mapping_game_data: dict, tournament_game_data: dict, retrieved_game_data: dict):
    """Detecting game winning side could be finnicky. We have been told that the tournaments file
    should NOT be used to:
    - detect red/blue side teams -> use mapping_data instead
    - detect winners -> use get participant info event from game if possible

    So we are going to do a sanity check for the tournament data with the mapping data AND the actual
    game data that provides us the info using the `winningTeam` column.

    NOTE: winning team can also be missing from games, in which case, we will need to fall back
    to the tournament data for source of truth since we don't have game win info anywhere else.

    Args:
        mapping_game_data (dict)
        tournament_game_data (dict)
        retrieved_game_data (dict)
    """
    # get all the team IDs from the various sources
    # mapping data could be empty (17 with missing "100" values 19 with "200" values)
    mapping_blue_team_id = str(mapping_game_data.get("100", ""))
    mapping_red_team_id = str(mapping_game_data.get("200", ""))
    # all completed games are filtered already so we should have this data
    tournament_blue_team_info = tournament_game_data.get("teams")[0]
    tournament_red_team_info = tournament_game_data.get("teams")[1]
    tournament_blue_team_id = str(tournament_blue_team_info.get("id", ""))
    tournament_red_team_id = str(tournament_red_team_info.get("id", ""))
    # tournaments data provides outcomes
    if tournament_blue_team_info.get("result", {}).get("outcome") == "win":
        winning_side_from_tournament = STR_SIDE_MAPPING["blue"]
    else:
        winning_side_from_tournament = STR_SIDE_MAPPING["red"]

    # game data also provides outcomes, but can be missing
    game_end_winner = retrieved_game_data[-1].get("winningTeam", None)

    # if tournament data and game data match up, use either
    # otherwise fall back to tournament data for source of truth
    game_winner = get_game_winner(game_end_winner, winning_side_from_tournament)

    are_blue_teams_equal = non_empty_equal(mapping_blue_team_id, tournament_blue_team_id)
    are_red_teams_equal = non_empty_equal(mapping_red_team_id, tournament_red_team_id)

    team_blue_id = mapping_blue_team_id if are_blue_teams_equal else tournament_blue_team_id
    team_red_id = mapping_red_team_id if are_red_teams_equal else tournament_red_team_id

    return team_blue_id, team_red_id, game_winner


def get_game_winner(game_end_winner, winning_side_from_tournament) -> int:
    if game_end_winner and game_end_winner == winning_side_from_tournament:
        game_winner = int(game_end_winner)
    else:
        game_winner = winning_side_from_tournament
    return game_winner


def get_date(zulu_date_str: str):
    return datetime.strptime(zulu_date_str, TIME_FORMAT).date()


def get_game_info_event_data(game_json_data):
    """Gets all the relevant information from the `game_info` eventType."""
    game_start = game_json_data[0]
    game_end = game_json_data[-1]

    game_info_event_data = {}

    game_info_event_data["game_date"] = get_date(game_start["eventTime"])
    game_info_event_data["game_duration"] = game_end["gameTime"] / 1000  # ms -> seconds
    game_info_event_data["game_patch"] = game_start["gameVersion"]
    participant_data = get_game_info_participant_data(game_start["participants"])
    epic_monsters_killed_data = get_epic_monster_kills(game_json_data)
    (
        game_info_event_data["team_first_turret_destroyed"],
        game_info_event_data["lane_first_turret_destroyed"],
    ) = get_team_first_turret_destroyed(game_json_data)
    game_info_event_data["team_first_blood"] = get_team_first_blood(game_json_data)
    return dict(game_info_event_data, **participant_data, **epic_monsters_killed_data)


def get_game_info_participant_data(participants_data):
    """Get game participant info from the nested participants column
    within the `game_info` event.

    The data is consistently setup as the following:
        T1 Zeus 100 Gwen 1
        T1 Oner 100 Viego 2
        T1 Faker 100 Viktor 3
        T1 Gumayusi 100 Varus 4
        T1 Keria 100 Karma 5
        DRX Kingen 200 Aatrox 6
        DRX Pyosik 200 Hecarim 7
        DRX Zeka 200 Azir 8
        DRX Deft 200 Caitlyn 9
        DRX BeryL 200 Bard 10

    Thus, players 1-5 will always belong on the same team, and 6-10 on the other.
    """
    participant_data = {}
    for player, role in zip(participants_data, ROLES):
        # 1_100_top = T1 Zeus, 6_200_top = DRX Kingen etc.
        base_key = f"{player['participantID']}_{player['teamID']}_{role}"
        participant_data[base_key] = player["summonerName"]
        participant_data[f"{base_key}_champion"] = player["championName"]
    return participant_data


def get_status_update_event_data(game_json_data):
    pass


def get_team_first_turret_destroyed(game_json_data) -> int:
    """Outer turrets are first to go, so we want to use that info to get
    the team that had the first turret destroyed.

    LANE_MAPPING = {
        "top": 1,
        "mid": 2,
        "bot": 3,
    }
    """
    outer_turrets_destroyed = [
        event
        for event in game_json_data
        if event["eventType"] == BUILDING_DESTROYED
        and event["buildingType"] == TURRET
        and event["turretTier"] == Turret.OUTER.value
    ]
    outer_turrets_destroyed.sort(key=lambda x: x["eventTime"])
    first_turret_destroyed = outer_turrets_destroyed[0]
    destroyed_turret_lane = LANE_MAPPING.get(str(first_turret_destroyed["lane"]), None)
    # killerTeamID is usually NaN, and so is other info
    # we get assist info usually, but teamID is always there
    turret_destroyed_team = int(first_turret_destroyed["teamID"])  # this is the team that had its
    # turret destroyed, NOT the team that destroyed it.
    # so get the opposing team instead.
    team_first_turret_destroyed = 200 if turret_destroyed_team == 100 else 100
    return team_first_turret_destroyed, destroyed_turret_lane


def get_team_first_blood(game_json_data) -> int:
    """Get which team took first blood.
    Every game should have this stat so no sanity checks needed.
    """
    champion_kill_events = [event for event in game_json_data if event["eventType"] == CHAMPION_KILL]
    champion_kill_events.sort(key=lambda x: x["eventTime"])
    first_blood_event = champion_kill_events[0]
    team_id = int(first_blood_event["killerTeamID"])
    return team_id


def get_epic_monster_kills(game_json_data) -> Dict[str, Union[int, str, None]]:
    """Get data related to who slayed the first dragon and baron.
    Some games may be missing dragon or baron data since they didn't need to take
    the objective, so we set None to maintain data integrity.
    """
    epic_monster_kills_data = {}
    num_dragons_secured_red = 0
    num_dragons_secured_blue = 0
    num_barons_secured_blue = 0
    num_barons_secured_red = 0

    dragons_killed = [
        event
        for event in game_json_data
        if event["eventType"] == EPIC_MONSTER_KILL and event["monsterType"] == Monsters.DRAGON.value
    ]
    barons_killed = [
        event
        for event in game_json_data
        if event["eventType"] == EPIC_MONSTER_KILL and event["monsterType"] == Monsters.BARON.value
    ]
    heralds_killed = [
        event
        for event in game_json_data
        if event["eventType"] == EPIC_MONSTER_KILL and event["monsterType"] == Monsters.HERALD.value
    ]

    if dragons_killed:
        dragons_killed.sort(key=lambda x: x["eventTime"])
        first_dragon_event = dragons_killed[0]
        epic_monster_kills_data["team_first_dragon_kill"] = int(first_dragon_event["killerTeamID"]) or None
        epic_monster_kills_data["first_dragon_type"] = DRAGON_TYPE_MAPPINGS.get(
            str(first_dragon_event["dragonType"]), None
        )

        for dragon_event in dragons_killed:
            if int(dragon_event["killerTeamID"]) == 100:
                num_dragons_secured_blue += 1
            elif int(dragon_event["killerTeamID"]) == 200:
                num_dragons_secured_red += 1

        epic_monster_kills_data["num_dragons_secured_blue"] = num_dragons_secured_blue
        epic_monster_kills_data["num_dragons_secured_red"] = num_dragons_secured_red

    if barons_killed:
        barons_killed.sort(key=lambda x: x["eventTime"])
        first_baron_event = barons_killed[0]
        epic_monster_kills_data["team_first_baron_kill"] = int(first_baron_event["killerTeamID"]) or None

        for baron_event in barons_killed:
            if int(baron_event["killerTeamID"]) == 100:
                num_barons_secured_blue += 1
            elif int(baron_event["killerTeamID"]) == 200:
                num_barons_secured_red += 1

        epic_monster_kills_data["num_barons_secured_blue"] = num_barons_secured_blue
        epic_monster_kills_data["num_barons_secured_red"] = num_barons_secured_red

    return epic_monster_kills_data


def get_team_names(red_team_id: str, blue_team_id: str):
    return team_id_to_info.get(red_team_id, {}).get("team_name", "Unknown"), team_id_to_info.get(blue_team_id, {}).get(
        "team_name", "Unknown"
    )


def aggregate_game_data(year: Optional[str] = None):
    no_platform_id = set()

    with open(f"{LOL_ESPORTS_DATA_DIR}/tournaments.json", "r") as json_file:
        tournaments_data = json.load(json_file)

    with open(f"{LOL_ESPORTS_DATA_DIR}/mapping_data.json", "r") as json_file:
        mappings_data = json.load(json_file)
        mappings = {esports_game["esportsGameId"]: esports_game for esports_game in mappings_data}

    for tournament in tournaments_data:
        tournament_slug = tournament.get("slug", "")
        if os.path.isfile(f"{CREATED_DATA_DIR}/tournaments/{tournament_slug}.csv"):
            continue
        tournament_id = tournament.get("id", "")
        tournament_name = tournament.get("name", "")
        start_date = tournament.get("startDate", "")
        end_date = tournament.get("endDate", "")
        tournament_games_df_list = []

        for stage in tournament.get("stages", []):
            # there are 19 unique stage names
            stage_name = stage["name"]
            stage_slug = stage["slug"]

            for section in stage.get("sections", []):
                # there are 20 unique section names
                section_name = section["name"]

                for match in section.get("matches", []):
                    # exclude "unstarted" matches
                    if match.get("state") == "completed":
                        # mode is always "classic"
                        # there are matches with "unstarted" state and so are the games within it
                        # there are 1327 unstarted matches overall
                        for game in match.get("games", []):
                            # some games are "unneeded"
                            if game.get("state") == "completed":
                                try:
                                    game_id = game["id"]
                                    game_data_from_mapping = mappings[game_id]
                                    platform_game_id = game_data_from_mapping["platformGameId"]
                                    game_number = int(game["number"])
                                except KeyError:
                                    print(f"No platform game id for game {game_id}")
                                    no_platform_id.add(game_id)
                                    continue

                                print(
                                    f"Processing tournament: {tournament['name']}, stage: {stage_name}, game: {game_id} "
                                )
                                retrieved_game_data = get_game_data(platform_game_id)
                                team_blue, team_red, game_winner = get_team_side_data(
                                    mapping_game_data=game_data_from_mapping,
                                    tournament_game_data=game,
                                    retrieved_game_data=retrieved_game_data,
                                )
                                team_blue_name, team_red_name = get_team_names(team_blue, team_red)

                                base_game_info = {
                                    "tournament_id": tournament_id,
                                    "tournament_name": tournament_name,
                                    "tournament_slug": tournament_slug,
                                    "tournament_start_date": start_date,
                                    "tournament_end_date": end_date,
                                    "platform_game_id": platform_game_id,
                                    "game_id": game_id,
                                    "game_number": game_number,
                                    "stage_name": stage_name,
                                    "stage_slug": stage_slug,
                                    "section_name": section_name,
                                    "team_100_blue_id": team_blue,
                                    "team_100_blue_name": team_blue_name,
                                    "team_200_red_id": team_red,
                                    "team_200_red_name": team_red_name,
                                    "game_winner": game_winner,
                                }

                                game_info_event_data = get_game_info_event_data(retrieved_game_data)
                                # TODO: get game status update event data for player data and
                                # add to all_game_info_data
                                all_game_info_data = dict(base_game_info, **game_info_event_data)
                                game_df = pd.DataFrame(all_game_info_data)
                                tournament_games_df_list.append(game_df)

        tournament_df = pd.concat(tournament_games_df_list, ignore_index=True)
        tournament_df.to_csv(f"{CREATED_DATA_DIR}/tournaments/{tournament_slug}.csv", index=False)


if __name__ == "__main__":
    # game https://www.youtube.com/watch?v=gapSIdUT8Us
    tournament_to_slug_mapping = get_tournament_to_stage_slug_mapping()
    print(len(tournament_to_slug_mapping))
    team_id_to_info = get_team_id_to_info_mapping()
    print(len(team_id_to_info))
    # aggregate_game_data("109511549831443335")  # LCS Challengers - only 2 tournaments, good to test
