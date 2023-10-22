import gzip
import json
import logging
import os
import shutil
from collections import defaultdict
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from constants import (
    BLUE_CHAMPION_COLUMNS,
    BUILDING_DESTROYED,
    CHAMPION_KILL,
    CREATED_DATA_DIR,
    DRAGON_TYPE_MAPPINGS,
    EPIC_MONSTER_KILL,
    GAME_INFO,
    GAMES_DIR,
    LANE_MAPPING,
    LOL_ESPORTS_DATA_DIR,
    PARTICIPANT_BASE_INFO,
    PARTICIPANT_BASE_INFO_LPL,
    PARTICIPANT_GAME_STATS,
    PARTICIPANT_GENERAL_STATS,
    RED_CHAMPION_COLUMNS,
    ROLES,
    S3_BUCKET_URL,
    STATS_UPDATE,
    STR_SIDE_MAPPING,
    TEAM_ID_TO_INFO_MAPPING_PATH,
    TEAM_STATS,
    TOURNAMENT_TO_SLUGS_MAPPING_PATH,
    TURRET,
    ExperienceTimers,
    Monsters,
    Turret,
)

# Logging configuration
logging.basicConfig(level=logging.INFO)


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


def get_team_side_by_value(value: int) -> str:
    for key, val in STR_SIDE_MAPPING.items():
        if val == value:
            return key
    return None


def get_game_data(platform_game_id: str):
    """A modified version of the method provided by Riot/AWS for downloading game data.
    Reads from local if available, else downloads, writes to file, and returns json.

    Args:
        file_name (str): game file name
    """
    if os.path.exists(f"{GAMES_DIR}/{platform_game_id}.json"):
        try:
            with open(f"{GAMES_DIR}/{platform_game_id}.json", "r") as f:
                json_data = json.load(f)
                print(f"{platform_game_id} - game data loaded! ---")
                return json_data
        except Exception as e:
            print("Error:", e)
    else:
        response = requests.get(f"{S3_BUCKET_URL}/{platform_game_id}.json.gz")
        if response.status_code == 200:
            try:
                gzip_bytes = BytesIO(response.content)
                with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                    with open(f"{GAMES_DIR}/{platform_game_id}.json", "wb") as output_file:
                        shutil.copyfileobj(gzipped_file, output_file)
                        print(f"{platform_game_id}.json written")

                with open(f"{GAMES_DIR}/{platform_game_id}.json", "r") as f:
                    json_data = json.load(f)
                    print(f"{platform_game_id} - game data downloaded & loaded! ---")
                    return json_data
            except Exception as e:
                print("Error:", e)
        else:
            print(f"Failed to request {platform_game_id} from S3")


def get_direct_game_data(platform_game_id: str):
    """Reads game data from S3 without saving it locally.

    Args:
        platform_game_id (str): The platform game ID.
    """
    if os.path.exists(f"{GAMES_DIR}/{platform_game_id}.json"):
        try:
            with open(f"{GAMES_DIR}/{platform_game_id}.json", "r") as f:
                json_data = json.load(f)
                print(f"{platform_game_id} - game data loaded! ---")
                return json_data
        except Exception as e:
            print("Error:", e)
    else:
        try:
            # Download the compressed JSON data from S3
            response = requests.get(f"{S3_BUCKET_URL}/{platform_game_id}.json.gz")
            if response.status_code == 200:
                gzip_bytes = BytesIO(response.content)
                with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                    json_data = json.load(gzipped_file)

                print(f"{platform_game_id} - game data downloaded & loaded! ---")
                return json_data
            else:
                print(f"Failed to request {platform_game_id} from S3")
        except Exception as e:
            print("Error:", e)


def get_team_side_data(mapping_game_data: dict, game_end_data: dict):
    """Detecting game winning side could be finnicky. We have been told that the tournaments file
    should NOT be used to:
    - detect red/blue side teams -> use mapping_data instead
    - detect winners -> use get participant info event from game if possible

    NOTE: winning team can also be missing from games

    Args:
        mapping_game_data (dict)
        retrieved_game_data (dict)
    """
    team_side_data = {}
    # get all the team IDs from the various sources
    # mapping data could be empty (17 with missing "100" values 19 with "200" values)
    team_mapping = mapping_game_data.get("teamMapping", {})
    mapping_blue_team_id = str(team_mapping.get("100", "Unknown"))
    mapping_red_team_id = str(team_mapping.get("200", "Unknown"))

    # game data also provides outcomes, but can be missing
    game_end_winner = int(game_end_data.get("winningTeam", 0))

    team_side_data["team_100_blue_id"] = mapping_blue_team_id
    team_side_data["team_200_red_id"] = mapping_red_team_id
    team_blue_name, team_red_name = get_team_names(mapping_blue_team_id, mapping_red_team_id)
    team_side_data["team_100_blue_name"] = team_blue_name
    team_side_data["team_200_red_name"] = team_red_name

    team_side_data["game_winner"] = game_end_winner

    return team_side_data


def get_game_event_data(game_json_data, mappings_data):
    """Gets all the relevant information from the `game_info` eventType."""
    (
        game_info_events,
        turret_destroyed_events,
        champion_kill_events,
        dragon_events,
        baron_events,
        herald_events,
        stats_update_events,
    ) = get_filtered_events_from_game_data(game_json_data)

    is_game_info_available = bool(game_info_events)

    # some LPL games have no game_info events
    # must fall back to stats_update events
    if is_game_info_available:
        game_start = game_info_events[0]
    else:
        game_start = stats_update_events[0]

    game_end = game_json_data[-1]

    game_info_event_data = {}

    game_info_event_data["game_date"] = game_start["eventTime"].split("T")[0]
    game_info_event_data["game_duration"] = game_end["gameTime"] // 1000  # ms -> seconds
    game_info_event_data["game_patch"] = game_start.get("gameVersion", "unknown")

    team_side_data = get_team_side_data(
        mapping_game_data=mappings_data,
        game_end_data=game_end,
    )

    participant_data = get_game_participant_data(
        game_start["participants"], get_base_info=True, is_game_info_available=is_game_info_available
    )

    epic_monsters_killed_data = get_epic_monster_kills(
        dragon_kill_events=dragon_events, baron_kill_events=baron_events, herald_kill_events=herald_events
    )

    (
        game_info_event_data["team_first_turret_destroyed"],
        game_info_event_data["lane_first_turret_destroyed"],
    ) = get_team_first_turret_destroyed(turret_destroyed_events=turret_destroyed_events)

    game_info_event_data["team_first_blood"] = get_team_first_blood(champion_kill_events=champion_kill_events)

    #### Get stats updates
    # They are always in order of participantID 1-5, 5-10, then repeats
    # team data is always in order of 100, then 200.

    #### Game stats
    game_status_update_data = get_game_status_update_event_data(stats_update_events=stats_update_events)

    return dict(
        game_info_event_data,
        **team_side_data,
        **participant_data,
        **epic_monsters_killed_data,
        **game_status_update_data,
    )


def get_game_participant_data(
    participants_data: List[Dict[str, Any]],
    get_base_info: bool = False,
    get_stats_info: bool = False,
    time_stamp: Optional[str] = None,
    is_game_info_available: Optional[bool] = True,
) -> Dict[str, Any]:
    """Get game participant info from the nested participants column
    within the `game_info` event.

    Thus, players 1-5 will always belong on the same team, and 6-10 on the other.

    Games in 2023 have goldStats, but older data does not have this field!!!
    """
    game_participant_data = {}

    if is_game_info_available:
        BASE_INFO_LIST = PARTICIPANT_BASE_INFO
    else:
        BASE_INFO_LIST = PARTICIPANT_BASE_INFO_LPL

    for player_info, role in zip(participants_data, ROLES):
        # 1_100_top = T1 Zeus, 6_200_top = DRX Kingen etc.
        base_key = f"{player_info['participantID']}_{player_info['teamID']}_{role}"

        if get_base_info and all(key in player_info for key in BASE_INFO_LIST):
            for base_info in BASE_INFO_LIST:
                game_participant_data[
                    f"{base_key}_{'summonerName' if base_info == 'playerName' else base_info}"
                ] = player_info[base_info]

        if get_stats_info:
            for general_stat in PARTICIPANT_GENERAL_STATS:
                game_participant_data[f"{base_key}_{general_stat}_{time_stamp}"] = player_info[general_stat]

        if get_stats_info:
            player_stats = {stat["name"]: stat["value"] for stat in player_info["stats"]}
            for game_stat in PARTICIPANT_GAME_STATS:
                value = float(player_stats.get(game_stat, 0))
                game_participant_data[f"{base_key}_{game_stat}_{time_stamp}"] = value
                game_participant_data[f"{player_info['teamID']}_total_{game_stat}_{time_stamp}"] = (
                    game_participant_data.get(f"{player_info['teamID']}_total_{game_stat}_{time_stamp}", 0.0) + value
                )

    return game_participant_data


def get_game_team_data(teams_data: List[Dict[str, Any]], time_stamp: str) -> Dict[str, Any]:
    """Get game team info from the nested teams column
    within the `game_info` event.

    The data is consistently setup as the following:
        T1 100
        DRX 200
    """
    game_team_data = {}

    for team_info in teams_data:
        team_id = int(team_info["teamID"])
        side = get_team_side_by_value(team_id)

        for team_stat in TEAM_STATS:
            game_team_data[f"{team_id}_{side}_{team_stat}_{time_stamp}"] = int(team_info[team_stat])

    return game_team_data


def get_filtered_events_from_game_data(game_json_data):
    """Get events that are relevant to the game but in one go O(N) vs multiple O(N)s
    in each function to go over every event.

    Since we iterate over every event, items are appended in order and won't require
    sorting it again."""
    # particular data events
    turret_destroyed_events = []
    champion_kill_events = []
    dragon_events = []
    baron_events = []
    herald_events = []
    stats_update_events = []
    game_info_events = []

    for event in game_json_data:
        if event["eventType"] == GAME_INFO:
            game_info_events.append(event)
        if event["eventType"] == STATS_UPDATE:
            stats_update_events.append(event)
        if event["eventType"] == BUILDING_DESTROYED and event["buildingType"] == TURRET:
            turret_destroyed_events.append(event)
        if event["eventType"] == CHAMPION_KILL:
            champion_kill_events.append(event)
        if event["eventType"] == EPIC_MONSTER_KILL and event.get("monsterType", "") == Monsters.DRAGON.value:
            dragon_events.append(event)
        if event["eventType"] == EPIC_MONSTER_KILL and event.get("monsterType", "") == Monsters.BARON.value:
            baron_events.append(event)
        if event["eventType"] == EPIC_MONSTER_KILL and event.get("monsterType", "") == Monsters.HERALD.value:
            herald_events.append(event)

    return (
        game_info_events,
        turret_destroyed_events,
        champion_kill_events,
        dragon_events,
        baron_events,
        herald_events,
        stats_update_events,
    )


def find_nearest_event(stats_update_events: List[Dict[str, Any]], target_time: int) -> Dict[str, Any]:
    """Find the nearest event to the given time stamp.
    This is used to get the participant and team stats for a game.
    """
    left, right = 0, len(stats_update_events) - 1
    while left <= right:
        mid = (left + right) // 2
        mid_event_game_time = int(stats_update_events[mid]["gameTime"]) // 1000

        if mid_event_game_time == target_time:
            return stats_update_events[mid]
        elif mid_event_game_time > target_time:
            right = mid - 1
        else:
            left = mid + 1

    if left >= len(stats_update_events):
        return stats_update_events[right]
    elif right < 0:
        return stats_update_events[left]
    else:
        return (
            stats_update_events[left]
            if abs((int(stats_update_events[left]["gameTime"]) // 1000) - target_time)
            < abs((int(stats_update_events[right]["gameTime"]) // 1000) - target_time)
            else stats_update_events[right]
        )


def get_game_status_update_event_data(stats_update_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get the late game and end game stats from the stats update events.
    This is the last event in the stats update events list.
    """
    game_status_data = {}

    # only include timers if they are at least 5 mins off the actual game_end time
    # otherwise just use game_end time since the data won't be drastically different
    GAME_TIMES = [
        ExperienceTimers.FIVE_MINS.value,
        ExperienceTimers.TEN_MINS.value,
        ExperienceTimers.FIFTEEN_MINS.value,
    ]

    for time_stamp in GAME_TIMES:
        event = find_nearest_event(stats_update_events=stats_update_events, target_time=time_stamp)
        participants_data = event["participants"]
        teams_data = event["teams"]

        participant_stats_data = get_game_participant_data(
            participants_data=participants_data, get_stats_info=True, time_stamp=str(time_stamp)
        )
        team_stats_data = get_game_team_data(teams_data=teams_data, time_stamp=str(time_stamp))
        game_status_data.update(participant_stats_data)
        game_status_data.update(team_stats_data)

    # last stat update
    end_game_stats = stats_update_events[-1]
    participants_data = end_game_stats["participants"]
    teams_data = end_game_stats["teams"]

    # participant stats collection
    participant_stats_data = get_game_participant_data(
        participants_data=participants_data, get_stats_info=True, time_stamp="game_end"
    )
    # team stats collection
    team_stats_data = get_game_team_data(teams_data=teams_data, time_stamp="game_end")
    end_game_info = dict(participant_stats_data, **team_stats_data)
    return dict(game_status_data, **end_game_info)


def get_team_first_turret_destroyed(turret_destroyed_events) -> int:
    """Outer turrets are first to go, so we want to use that info to get
    the team that had the first turret destroyed.

    LANE_MAPPING = {
        "top": 1,
        "mid": 2,
        "bot": 3,
    }
    """
    outer_turrets_destroyed = [
        turret_event for turret_event in turret_destroyed_events if turret_event["turretTier"] == Turret.OUTER.value
    ]
    first_turret_destroyed = outer_turrets_destroyed[0]
    destroyed_turret_lane = LANE_MAPPING.get(str(first_turret_destroyed["lane"]))
    # killerTeamID is usually NaN, and so is other info
    # we get assist info usually, but teamID is always there
    turret_destroyed_team = int(first_turret_destroyed["teamID"])  # this is the team that had its
    # turret destroyed, NOT the team that destroyed it.
    # so get the opposing team instead.
    team_first_turret_destroyed = 200 if turret_destroyed_team == 100 else 100
    return team_first_turret_destroyed, destroyed_turret_lane


def get_team_first_blood(champion_kill_events) -> int:
    """Get which team took first blood.
    Every game should have this stat so no sanity checks needed.
    """
    first_blood_event = champion_kill_events[0]
    team_id = int(first_blood_event["killerTeamID"])
    return team_id


def get_dragon_kills_data(
    monster_kills_dict: Dict[str, Union[int, str, None]], dragon_kill_events: Optional[List[Dict[str, Any]]]
) -> None:
    """Get dragon kill data from the epic monster kill data. 0 placeholder for no first dragon"""
    if dragon_kill_events:
        first_dragon_event = dragon_kill_events[0]
        monster_kills_dict.update(
            {
                "team_first_dragon_kill": int(first_dragon_event["killerTeamID"]),
                "first_dragon_type": DRAGON_TYPE_MAPPINGS.get(str(first_dragon_event["dragonType"])),
            }
        )
    else:
        monster_kills_dict.update(
            {
                "team_first_dragon_kill": 0,
                "first_dragon_type": DRAGON_TYPE_MAPPINGS.get("unknown"),
            }
        )


def is_dragon_soul_collected(
    monster_kills_dict: Dict[str, Union[int, str, None]],
    dragon_kill_events: Optional[List[Dict[str, Any]]],
):
    """Check which dragon soul is collected by a team, if it is collected at all. This is also quite gamechanging
    based on which elemental soul it is. First team to reach 4 dragon slayer stacks gets it.

    Mapping with rank is available in consts -> DRAGON_TYPE_MAPPINGS"""

    if not dragon_kill_events:
        monster_kills_dict.update(
            {"is_dragon_soul_collected": 0, "team_first_dragon_soul": 0, "dragon_soul_collected": 0}
        )
        return

    dragon_type_counter = defaultdict(int)
    blue_dragon_kills, red_dragon_kills = 0, 0

    for dragon_event in dragon_kill_events:
        dragon_type = dragon_event.get("dragonType", "unknown")
        if dragon_type in DRAGON_TYPE_MAPPINGS:
            dragon_type_counter[dragon_type] += 1

        killer_team_id = int(dragon_event.get("killerTeamID"))
        if killer_team_id == 100:
            blue_dragon_kills += 1
        elif killer_team_id == 200:
            red_dragon_kills += 1

    has_blue_taken_soul = blue_dragon_kills >= 4
    has_red_taken_soul = red_dragon_kills >= 4

    if has_blue_taken_soul or has_red_taken_soul:
        dominant_dragon_type = max(dragon_type_counter, key=dragon_type_counter.get)
        monster_kills_dict.update(
            {
                "is_dragon_soul_collected": 1,
                "team_first_dragon_soul": 200 if has_red_taken_soul else 100,
                "dragon_soul_collected": DRAGON_TYPE_MAPPINGS.get(dominant_dragon_type),
            }
        )
    else:
        monster_kills_dict.update(
            {"is_dragon_soul_collected": 0, "team_first_dragon_soul": 0, "dragon_soul_collected": 0}
        )


def is_elder_collected(
    monster_kills_dict: Dict[str, Union[int, str, None]], dragon_kill_events: Optional[List[Dict[str, Any]]]
) -> bool:
    """Check if elder is collected by a team, since it's a huge game-changing buff for the team taking it."""

    is_elder_dragon_collected = 0
    team_first_elder_dragon = 0

    if dragon_kill_events:
        elder_kill_events = [event for event in dragon_kill_events if event["dragonType"] == "elder"]
        if elder_kill_events:
            first_elder_event = elder_kill_events[0]
            is_elder_dragon_collected = 1
            team_first_elder_dragon = int(first_elder_event["killerTeamID"])

    monster_kills_dict.update(
        {"is_elder_dragon_collected": is_elder_dragon_collected, "team_first_elder_dragon": team_first_elder_dragon}
    )


def get_baron_kills_data(monster_kills_dict: Dict[str, Union[int, str, None]], baron_kill_events) -> None:
    """Get baron kill data from the epic monster kill data. 0 placeholder for no first baron"""
    team_first_baron_kill = int(baron_kill_events[0]["killerTeamID"]) if baron_kill_events else 0
    monster_kills_dict["team_first_baron_kill"] = team_first_baron_kill


def get_herald_kills_data(monster_kills_dict: Dict[str, Union[int, str, None]], herald_kill_events) -> None:
    """Get herald kill data from the epic monster kill data. 0 place holder for no first herald"""
    num_heralds_secured_blue = 0
    num_heralds_secured_red = 0

    first_herald_event = herald_kill_events[0]
    monster_kills_dict["team_first_herald_kill"] = int(first_herald_event["killerTeamID"]) or 0

    # herald data is not available in stats_update teams data
    for herald_event in herald_kill_events:
        if int(herald_event["killerTeamID"]) == 100:
            num_heralds_secured_blue += 1
        elif int(herald_event["killerTeamID"]) == 200:
            num_heralds_secured_red += 1

    monster_kills_dict["num_heralds_secured_blue"] = num_heralds_secured_blue
    monster_kills_dict["num_heralds_secured_red"] = num_heralds_secured_red


def get_epic_monster_kills(
    dragon_kill_events, baron_kill_events, herald_kill_events
) -> Dict[str, Union[int, str, None]]:
    """Get data related to who slayed the first dragon and baron.
    Some games may be missing dragon or baron data since they didn't need to take
    the objective, so we set None to maintain data integrity.
    """
    epic_monster_kills_data = {}

    get_dragon_kills_data(epic_monster_kills_data, dragon_kill_events)
    get_baron_kills_data(epic_monster_kills_data, baron_kill_events)
    get_herald_kills_data(epic_monster_kills_data, herald_kill_events)
    is_dragon_soul_collected(epic_monster_kills_data, dragon_kill_events)
    is_elder_collected(epic_monster_kills_data, dragon_kill_events)

    return epic_monster_kills_data


def get_team_names(red_team_id: str, blue_team_id: str):
    return team_id_to_info.get(red_team_id, {}).get("team_name", "Unknown"), team_id_to_info.get(blue_team_id, {}).get(
        "team_name", "Unknown"
    )


def get_league_tournaments(league_id: str) -> List[str]:
    with open(f"{LOL_ESPORTS_DATA_DIR}/leagues.json", "r") as f:
        leagues_data = json.load(f)
        for league in leagues_data:
            if league["id"] == league_id:
                return [tournament["id"] for tournament in league["tournaments"]]


def aggregate_game_data(year: Optional[str] = None, by_tournament_id: Optional[str] = None) -> Tuple[str, str]:
    with open(f"{LOL_ESPORTS_DATA_DIR}/tournaments.json", "r") as json_file:
        tournaments_data = json.load(json_file)
        if by_tournament_id:
            tournaments_data = [
                tournament
                for tournament in tournaments_data
                if tournament["id"] == by_tournament_id and str(tournament["startDate"]).startswith(year)
            ]

    with open(f"{LOL_ESPORTS_DATA_DIR}/mapping_data.json", "r") as json_file:
        mappings_data = json.load(json_file)
        mappings = {esports_game["esportsGameId"]: esports_game for esports_game in mappings_data}

    if not tournaments_data:
        print(f"No tournament data for tournament ID: {by_tournament_id}")
        return "", ""

    for tournament in tournaments_data:
        tournament_slug = tournament.get("slug", "")
        league_id = tournament.get("leagueId", "")
        if os.path.isfile(f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}.csv"):
            return league_id, tournament_slug
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
                                    continue

                                print(
                                    f"Processing tournament: {tournament['name']}, stage: {stage_name}, game: {game_id}"
                                )
                                retrieved_game_data = get_direct_game_data(platform_game_id)

                                if not retrieved_game_data:
                                    continue

                                base_game_info = {
                                    "league_id": league_id,
                                    "tournament_id": tournament_id,
                                    "tournament_name": tournament_name,
                                    "tournament_slug": tournament_slug,
                                    "tournament_start_date": pd.to_datetime(start_date),
                                    "tournament_end_date": pd.to_datetime(end_date),
                                    "platform_game_id": platform_game_id,
                                    "game_id": game_id,
                                    "game_number": game_number,
                                    "stage_name": stage_name,
                                    "stage_slug": stage_slug,
                                    "section_name": section_name,
                                }

                                game_event_data = get_game_event_data(retrieved_game_data, game_data_from_mapping)
                                all_game_info_data = dict(base_game_info, **game_event_data)
                                game_df = pd.DataFrame([all_game_info_data])
                                tournament_games_df_list.append(game_df)
                                print(
                                    f"Processing tournament: {tournament['name']}, stage: {stage_name}, game: {game_id} - ✅",
                                    end="\n\n",
                                )

        if not os.path.exists(f"{CREATED_DATA_DIR}/mapped-games/{league_id}"):
            os.makedirs(f"{CREATED_DATA_DIR}/mapped-games/{league_id}")
        tournament_df = pd.concat(tournament_games_df_list, ignore_index=True)
        tournament_df.sort_values(by=["game_date", "game_number"], inplace=True)
        tournament_df.to_csv(f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}.csv", index=False)
        print(f"Completed processing league: {league_id} tournament: {tournament_slug} ✅", end="\n------------\n\n")
        return league_id, tournament_slug


def calculate_champion_stats_for_role(data, col, side):
    role_stats = {}
    for _, row in data.iterrows():
        champ = row[col]
        winner = row["game_winner"]
        if champ not in role_stats:
            role_stats[champ] = {"games_played": 0, "games_won": 0}
        role_stats[champ]["games_played"] += 1
        if winner == side:
            role_stats[champ]["games_won"] += 1

    role_data = []
    for champ, stats in role_stats.items():
        frequency = stats["games_played"]
        win_rate = (stats["games_won"] / stats["games_played"]) * 100
        role_data.append({champ: frequency, "winRate": win_rate})

    # sort by frequency
    role_data = sorted(role_data, key=lambda x: list(x.values())[0], reverse=True)
    return role_data


def get_champion_occurrences_from_aggregate_tournament(league_id: str, tournament_slug: str) -> None:
    # f"{CREATED_DATA_DIR}/mapped-games/109518549825754242/nacl_qualifiers_2_summer_2023.csv"
    # if os.path.exists(f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}_champion_mapping.json"):
    #     return

    tournament_df = pd.read_csv(f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}.csv")
    champion_mapping = {"100": {}, "200": {}}

    for blue_col in BLUE_CHAMPION_COLUMNS:
        role = blue_col.split("_")[2]
        champion_mapping["100"][role] = calculate_champion_stats_for_role(tournament_df, blue_col, 100)
    for red_col in RED_CHAMPION_COLUMNS:
        role = red_col.split("_")[2]
        champion_mapping["200"][role] = calculate_champion_stats_for_role(tournament_df, red_col, 200)

    # Save the champion mapping dictionary as a JSON file
    with open(f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}_champion_mapping.json", "w") as file:
        json.dump(champion_mapping, file)


def concatenate_csv_files(directory_path, output_file) -> None:
    """
    Concatenate all .csv files in a directory with games from 2023 into a single CSV file.
    Targets the games from following regions: LPL, LEC, LCK, LCS, PCS, VCS, CBLOL, LJL, LLA

    Parameters:
    - directory_path (str): Path to the directory containing the CSV files.
    - output_file (str): Path to the output CSV file.
    """
    specific_leagues = [
        "98767991299243165",  # LCS
        "98767991310872058",  # LCK
        "98767991314006698",  # LPL
        "98767991302996019",  # LEC
        "104366947889790212",  # PCS
        "107213827295848783",  # VCS
        "98767991332355509",  # CBLOL
        "98767991349978712",  # LJL
        "101382741235120470",  # LLA
        "98767991325878492",  # MSI
        "98767975604431411",  # Worlds
    ]

    csv_files = [
        (league_id, f)
        for league_id in specific_leagues
        for f in os.listdir(f"{directory_path}/{league_id}")
        if f.endswith(".csv") and ("2022" in f or "2023" in f)
    ]
    dfs = []

    for league_id, csv_file in csv_files:
        df = pd.read_csv(os.path.join(directory_path, league_id, csv_file))
        dfs.append(df)

    concatenated_df = pd.concat(dfs, ignore_index=True)
    concatenated_df.to_csv(output_file, index=False)
    print(f"All CSV files concatenated and saved to {output_file}")


def get_num_rows_to_swap(tournament_df: pd.DataFrame):
    rows_to_swap = set()
    for index, row in tournament_df.iterrows():
        blue_team_code = get_team_code_from_id(str(row["team_100_blue_id"]))
        red_team_code = get_team_code_from_id(str(row["team_200_red_id"]))

        # Check if player summoner names start with the respective team codes
        if not (
            row["1_100_top_summonerName"].startswith(blue_team_code)
            and row["2_100_jng_summonerName"].startswith(blue_team_code)
        ):
            rows_to_swap.add(index)
        elif not (
            row["6_200_top_summonerName"].startswith(red_team_code)
            and row["7_200_jng_summonerName"].startswith(red_team_code)
        ):
            rows_to_swap.add(index)
    return rows_to_swap


def delete_games_directory(games_dir: str):
    try:
        shutil.rmtree(games_dir)
        print(f"Directory '{games_dir}' and its contents have been deleted.")
    except OSError as e:
        print(f"Error: {e}")


def get_team_code_from_id(team_id):
    return team_id_to_info.get(str(team_id), {}).get("team_code", "Unknown")


if __name__ == "__main__":
    #### Setup base https://www.youtube.com/watch?v=gapSIdUT8Us
    tournament_to_slug_mapping = get_tournament_to_stage_slug_mapping()
    print(len(tournament_to_slug_mapping))
    team_id_to_info = get_team_id_to_info_mapping()
    print(len(team_id_to_info))

    #### Data Aggregation
    # league_ids = [
    #     "110372322609949919",
    #     "110371976858004491",
    #     "105549980953490846",
    #     "98767991335774713",
    #     "106827757669296909",
    #     "108203770023880322",
    #     "109545772895506419",
    #     "105266108767593290",
    #     "105266111679554379",
    #     "105266106309666619",
    #     "105266091639104326",
    #     "105266074488398661",
    #     "105266088231437431",
    #     "105266094998946936",
    #     "105266101075764040",
    #     "107407335299756365",
    #     "105266098308571975",
    #     "105266103462388553",
    #     "100695891328981122",
    #     "98767991295297326",
    #     "98767975604431411",
    #     "98767991325878492",
    #     "107213827295848783",
    #     "98767991343597634",
    #     "104366947889790212",
    #     "98767991314006698",
    #     "101382741235120470",
    #     "98767991349978712",
    #     "98767991302996019",
    #     "105709090213554609",
    #     "98767991355908944",
    #     "98767991310872058",
    #     "98767991332355509",
    #     "107898214974993351",
    #     "109518549825754242",
    #     "109511549831443335",
    #     "98767991299243165",
    # ]
    # specific_leagues = [
    #     "98767991299243165",  # LCS
    #     "98767991310872058",  # LCK
    #     "98767991314006698",  # LPL
    #     "98767991302996019",  # LEC
    #     "104366947889790212",  # PCS
    #     "107213827295848783",  # VCS
    #     "98767991332355509",  # CBLOL
    #     "98767991349978712",  # LJL
    #     "101382741235120470",  # LLA
    #     "98767991325878492",  # MSI
    #     "98767975604431411",  # Worlds
    # ]
    # for league_id in specific_leagues:
    #     league_tournaments = get_league_tournaments(league_id=league_id)
    #     print(f"Total tournaments: {len(league_tournaments)}")
    #     count = 0
    #     for tournament_id in league_tournaments:
    #         league_id, tournament_slug = aggregate_game_data(by_tournament_id=tournament_id, year="2022")
    #         if league_id and tournament_slug:
    #             count += 1
    #             get_champion_occurrences_from_aggregate_tournament(league_id=league_id, tournament_slug=tournament_slug)
    #         # delete_games_directory(GAMES_DIR)
    #     print(f"Total tournaments processed: {count}/{len(league_tournaments)}")

    #### Concatenate all CSV files
    # concatenate_csv_files(
    #     directory_path=f"{CREATED_DATA_DIR}/mapped-games",
    #     output_file=f"{CREATED_DATA_DIR}/mapped-combined-games.csv",
    # )
