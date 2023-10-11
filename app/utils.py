import csv
import gzip
import json
import os
import shutil
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, Optional, Union

import requests
from constants import (
    BUILDING_DESTROYED,
    CHAMPION_KILL,
    CREATED_DATA_DIR,
    EPIC_MONSTER_KILL,
    GAMES_DIR,
    LOL_ESPORTS_DATA_DIR,
    ROLES,
    S3_BUCKET_URL,
    STR_SIDE_MAPPING,
    TIME_FORMAT,
    TURRET,
    Monsters,
    Turret,
)


def get_game_data(file_name: str):
    """A modified version of the method provided by Riot/AWS for downloading game data.
    Reads from local if available, else downloads, writes to file, and returns json.

    Args:
        file_name (str): game file name
    """
    if os.path.exists(f"{GAMES_DIR}/{file_name}.json"):
        with open(f"{GAMES_DIR}/{file_name}.json", "r") as f:
            json_data = json.load(f)
            return json_data
    else:
        response = requests.get(f"{S3_BUCKET_URL}/{file_name}.json.gz")
        if response.status_code == 200:
            try:
                gzip_bytes = BytesIO(response.content)
                with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                    with open(f"{GAMES_DIR}/{file_name}.json", "wb") as output_file:
                        shutil.copyfileobj(gzipped_file, output_file)
                    print(f"{file_name}.json written")
                    gzipped_content = gzipped_file.read().decode("utf-8")
                    json_data = json.loads(gzipped_content)
                    return json_data
            except Exception as e:
                print("Error:", e)
        else:
            print(f"Failed to download {file_name}")


def get_team_side_data(mapping_game_data: dict, tournament_game_data: dict, retrieved_game_data: dict):
    """Detecting game winning side could be finnicky. We have been told that the tournaments file
    should NOT be used to:
    - detect red/blue side teams -> use mapping_data instead
    - get participant info

    So we are going to do a sanity check for the tournament data with the mapping data AND the actual
    game data that provides us the info using the `winningTeam` column.

    NOTE: winning team can also be missing from games, in which case, we will need to fall back
    to the tournament data for source of truth.

    Args:
        mapping_game_data (dict)
        tournament_game_data (dict)
        retrieved_game_data (dict)
    """
    # get all the team IDs from the various sources
    mapping_blue_team_id = str(mapping_game_data.get("100", ""))
    mapping_red_team_id = str(mapping_game_data.get("200", ""))
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

    is_consistent = mapping_blue_team_id == tournament_blue_team_id and mapping_red_team_id == tournament_red_team_id

    team_blue_id = mapping_blue_team_id
    team_red_id = mapping_red_team_id

    return is_consistent, team_blue_id, team_red_id, game_winner


def get_game_winner(game_end_winner, winning_side_from_tournament):
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
    epic_monsters_killed_data = get_first_epic_monster_kills(game_json_data)
    game_info_event_data["team_first_turret_destroyed"] = get_team_first_turret_destroyed(game_json_data)
    game_info_event_data["team_first_blood"] = get_team_first_blood(game_json_data)
    return dict(game_info_event_data, **participant_data, **epic_monsters_killed_data)


def get_game_info_participant_data(participants_data):
    """Get game participant info from the nested participants column
    within the `game_info` event.
    """
    participant_data = {}
    for player, role in zip(participants_data, ROLES):
        base_key = f"{player['participantID']}_{player['teamID']}_{role}"
        participant_data[base_key] = player["summonerName"]
        participant_data[f"{base_key}_champion"] = player["championName"]


def get_status_update_event_data(game_json_data):
    pass


def get_team_first_turret_destroyed(game_json_data) -> int:
    """Outer turrets are first to go, so we want to use that info to get
    the team that had the first turret destroyed.
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
    team_id = int(first_turret_destroyed["teamID"])
    return team_id


def get_team_first_blood(game_json_data) -> int:
    """Get which team took first blood.
    Every game should have this stat so no sanity checks needed.
    """
    champion_kill_events = [event for event in game_json_data if event["eventType"] == CHAMPION_KILL]
    champion_kill_events.sort(key=lambda x: x["eventTime"])
    first_blood_event = champion_kill_events[0]
    team_id = int(first_blood_event["killerTeamID"])
    return team_id


def get_first_epic_monster_kills(game_json_data) -> Dict[str, Union[int, str, None]]:
    """Get data related to who slayed the first dragon and baron.
    Some games may be missing dragon or baron data since they didn't need to take
    the objective, so we set None to maintain data integrity.
    """
    epic_monster_kills_data = {}
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
    if dragons_killed:
        dragons_killed.sort(key=lambda x: x["eventTime"])
        first_dragon_event = dragons_killed[0]
        epic_monster_kills_data["team_first_dragon_kill"] = int(first_dragon_event["killerTeamID"])
        epic_monster_kills_data["first_dragon_type"] = str(first_dragon_event["dragonType"])
    else:
        epic_monster_kills_data["team_first_dragon_kill"] = None
        epic_monster_kills_data["first_dragon_type"] = None
    if barons_killed:
        barons_killed.sort(key=lambda x: x["eventTime"])
        first_baron_event = barons_killed[0]
        epic_monster_kills_data["team_first_baron_kill"] = int(first_baron_event["killerTeamID"])
    else:
        epic_monster_kills_data["team_first_baron_kill"] = None

    return epic_monster_kills_data


def aggregate_game_data(league_id: Optional[str] = None):
    inconsistent_mapping_games = set()
    no_platform_id = set()

    with open(f"{CREATED_DATA_DIR}/updated-leagues.csv") as csv_file:
        reader = csv.DictReader(csv_file)
        if league_id:
            leagues_data = {row["League ID"]: row for row in reader if row["League ID"] == league_id}
        else:
            leagues_data = {row["League ID"]: row for row in reader}

    with open(f"{LOL_ESPORTS_DATA_DIR}/tournaments.json", "r") as json_file:
        tournaments_data = json.load(json_file)

    with open(f"{CREATED_DATA_DIR}/filtered_mapping_data.csv", "r") as csv_file:
        reader = csv.DictReader(csv_file)
        mappings = {esports_game["esportsGameId"]: esports_game for esports_game in reader}

    for league in leagues_data.values():
        if not os.path.exists(f"{CREATED_DATA_DIR}/leagues/{league['League Slug']}"):
            os.makedirs(f"{CREATED_DATA_DIR}/leagues/{league['League Slug']}")

        print(f"Processing league: {league['League Name']} — {league['League Slug']}")
        league_tournament_ids = league["Tournaments"]

        for tournament_id in league_tournament_ids:
            for tournament in tournaments_data:
                if tournament.get("id", "") == tournament_id:
                    tournament_name = tournament.get("name", "")
                    tournament_slug = tournament.get("slug", "")
                    start_date = tournament.get("startDate", "")
                    end_date = tournament.get("endDate", "")
                    print(f"Processing tournament: {tournament_name} — {tournament_slug}")

                    for stage in tournament.get("stages", []):
                        # there are 19 unique stage names
                        stage_name = stage["name"]
                        stage_slug = stage["slug"]
                        print(f"Processing {tournament['name']} stage: {stage_name} — {stage_slug}")

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
                                                game_number = game["number"]
                                            except KeyError:
                                                print(f"No platform game id for game {game_id}")
                                                no_platform_id.add(game_id)
                                                continue

                                            retrieved_game_data = get_game_data(platform_game_id)
                                            is_consistent, team_blue, team_red, game_winner = get_team_side_data(
                                                mapping_game_data=game_data_from_mapping,
                                                tournament_game_data=game,
                                                retrieved_game_data=retrieved_game_data,
                                            )

                                            if not is_consistent:
                                                inconsistent_mapping_games.add(game_id)
                                                continue

                                            tournament_game_info = {
                                                "tournament_id": tournament_id,
                                                "tournament_name": tournament_name,
                                                "tournament_start_date": start_date,
                                                "tournament_end_date": end_date,
                                                "platform_game_id": platform_game_id,
                                                "game_id": game_id,
                                                "game_number": game_number,
                                                "stage_name": stage_name,
                                                "stage_slug": stage_slug,
                                                "section_name": section_name,
                                                "team_blue": team_blue,
                                                "team_red": team_red,
                                                "game_winner": game_winner,
                                            }

                                            game_info_event_data = get_game_info_event_data(retrieved_game_data)


if __name__ == "__main__":
    # game https://www.youtube.com/watch?v=kAVm8zg-ZSU
    game_data = get_game_data("ESPORTS3294091")
    # aggregate_game_data("109511549831443335")  # LCS Challengers - only 2 tournaments, good to test
