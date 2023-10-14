import gzip
import json
import logging
import os
import shutil
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Union

import pandas as pd
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
    PARTICIPANT_BASE_INFO,
    PARTICIPANT_GAME_STATS,
    PARTICIPANT_GENERAL_STATS,
    PARTICIPANT_GOLD_STATS,
    ROLES,
    S3_BUCKET_URL,
    STATS_UPDATE,
    STR_SIDE_MAPPING,
    TEAM_ID_TO_INFO_MAPPING_PATH,
    TEAM_STATS,
    TIME_FORMAT,
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


def get_game_data(platform_game_id: str):
    """A modified version of the method provided by Riot/AWS for downloading game data.
    Reads from local if available, else downloads, writes to file, and returns json.

    Args:
        file_name (str): game file name
    """
    if os.path.exists(f"{GAMES_DIR}/{platform_game_id}.json"):
        with open(f"{GAMES_DIR}/{platform_game_id}.json", "r") as f:
            json_data = json.load(f)
            print(f"{platform_game_id} - game data loaded! ---")
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

                with open(f"{GAMES_DIR}/{platform_game_id}.json", "r") as f:
                    json_data = json.load(f)
                    print(f"{platform_game_id} - game data downloaded & loaded! ---")
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


def get_game_date(zulu_time_stamp: str):
    return datetime.strptime(zulu_time_stamp, TIME_FORMAT).date()


def get_game_event_data(game_json_data):
    """Gets all the relevant information from the `game_info` eventType."""
    game_start = game_json_data[0]
    game_end = game_json_data[-1]

    game_info_event_data = {}

    game_info_event_data["game_date"] = get_game_date(game_start["eventTime"])
    game_info_event_data["game_duration"] = game_end["gameTime"] // 1000  # ms -> seconds
    game_info_event_data["game_patch"] = game_start["gameVersion"]

    participant_data = get_game_participant_data(game_start["participants"], get_base_info=True)

    (
        turret_destroyed_events,
        champion_kill_events,
        dragon_events,
        baron_events,
        herald_events,
        stats_update_events,
    ) = get_filtered_events_from_game_data(game_json_data)

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
        **participant_data,
        **epic_monsters_killed_data,
        **game_status_update_data,
    )


def get_game_participant_data(
    participants_data: List[Dict[str, Any]],
    get_base_info: bool = False,
    get_stats_info: bool = False,
    time_stamp: Optional[str] = None,
) -> Dict[str, Any]:
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
    game_participant_data = {}

    for player_info, role in zip(participants_data, ROLES):
        # 1_100_top = T1 Zeus, 6_200_top = DRX Kingen etc.
        base_key = f"{player_info['participantID']}_{player_info['teamID']}_{role}"
        gold_stats = player_info.get("goldStats", {})

        if get_base_info and all(key in player_info for key in PARTICIPANT_BASE_INFO):
            for base_info in PARTICIPANT_BASE_INFO:
                game_participant_data[f"{base_key}_{base_info}"] = player_info.get(base_info, None)

        if get_stats_info and all(key in player_info for key in PARTICIPANT_GENERAL_STATS):
            for general_stat in PARTICIPANT_GENERAL_STATS:
                game_participant_data[f"{base_key}_{general_stat}_{time_stamp}"] = player_info.get(general_stat, None)

        if get_stats_info and all(key in gold_stats for key in PARTICIPANT_GOLD_STATS):
            for gold_stat in PARTICIPANT_GOLD_STATS:
                value = float(gold_stats.get(gold_stat, 0))
                game_participant_data[f"{base_key}_{gold_stat}_{time_stamp}"] = value
                game_participant_data[f"{player_info['teamID']}_total_{gold_stat}_{time_stamp}"] = (
                    game_participant_data.get(f"{player_info['teamID']}_total_{gold_stat}_{time_stamp}", 0.0) + value
                )

        if get_stats_info:
            player_stats = {stat["name"]: stat["value"] for stat in player_info.get("stats", [])}
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

        if all(key in team_info for key in TEAM_STATS):
            for team_stat in TEAM_STATS:
                game_team_data[f"{team_id}_{team_stat}_{time_stamp}"] = int(team_info.get(team_stat, 0))

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

    for event in game_json_data:
        if event["eventType"] == STATS_UPDATE:
            stats_update_events.append(event)
        if event["eventType"] == BUILDING_DESTROYED and event["buildingType"] == TURRET:
            turret_destroyed_events.append(event)
        if event["eventType"] == CHAMPION_KILL:
            champion_kill_events.append(event)
        if event["eventType"] == EPIC_MONSTER_KILL and event["monsterType"] == Monsters.DRAGON.value:
            dragon_events.append(event)
        if event["eventType"] == EPIC_MONSTER_KILL and event["monsterType"] == Monsters.BARON.value:
            baron_events.append(event)
        if event["eventType"] == EPIC_MONSTER_KILL and event["monsterType"] == Monsters.HERALD.value:
            herald_events.append(event)

    return (
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
            if abs(stats_update_events[left]["gameTime"] - target_time)
            < abs(stats_update_events[right]["gameTime"] - target_time)
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
        # ExperienceTimers.FIVE_MINS.value,
        # ExperienceTimers.TEN_MINS.value,
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
        participants_data=participants_data, get_stats_info=True, time_stamp="end"
    )
    # team stats collection
    team_stats_data = get_game_team_data(teams_data=teams_data, time_stamp="end")
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
    destroyed_turret_lane = LANE_MAPPING.get(str(first_turret_destroyed["lane"]), None)
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


def get_dragon_kills_data(monster_kills_dict: Dict[str, Union[int, str, None]], dragon_kill_events) -> None:
    """Get dragon kill data from the epic monster kill data."""
    if dragon_kill_events:
        first_dragon_event = dragon_kill_events[0]
        monster_kills_dict["team_first_dragon_kill"] = int(first_dragon_event["killerTeamID"]) or None
        monster_kills_dict["first_dragon_type"] = DRAGON_TYPE_MAPPINGS.get(str(first_dragon_event["dragonType"]), None)


def get_baron_kills_data(monster_kills_dict: Dict[str, Union[int, str, None]], baron_kill_events) -> None:
    """Get baron kill data from the epic monster kill data."""
    if baron_kill_events:
        first_baron_event = baron_kill_events[0]
        monster_kills_dict["team_first_baron_kill"] = int(first_baron_event["killerTeamID"]) or None


def get_herald_kills_data(monster_kills_dict: Dict[str, Union[int, str, None]], herald_kill_events) -> None:
    """Get herald kill data from the epic monster kill data."""
    num_heralds_secured_blue = 0
    num_heralds_secured_red = 0

    if herald_kill_events:
        first_herald_event = herald_kill_events[0]
        monster_kills_dict["team_first_herald_kill"] = int(first_herald_event["killerTeamID"]) or None

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

    return epic_monster_kills_data


def get_team_names(red_team_id: str, blue_team_id: str):
    return team_id_to_info.get(red_team_id, {}).get("team_name", "Unknown"), team_id_to_info.get(blue_team_id, {}).get(
        "team_name", "Unknown"
    )


def aggregate_game_data(year: Optional[str] = None, by_tournament_id: Optional[str] = None):
    no_platform_id = set()

    with open(f"{LOL_ESPORTS_DATA_DIR}/tournaments.json", "r") as json_file:
        tournaments_data = json.load(json_file)
        if year:
            tournaments_data = [
                tournament for tournament in tournaments_data if str(tournament["startDate"]).startswith(year)
            ]
        if by_tournament_id:
            tournaments_data = [tournament for tournament in tournaments_data if tournament["id"] == by_tournament_id]

    with open(f"{LOL_ESPORTS_DATA_DIR}/mapping_data.json", "r") as json_file:
        mappings_data = json.load(json_file)
        mappings = {esports_game["esportsGameId"]: esports_game for esports_game in mappings_data}

    for tournament in tournaments_data:
        tournament_slug = tournament.get("slug", "")
        league_id = tournament.get("leagueId", "")
        if os.path.isfile(f"{CREATED_DATA_DIR}/aggregate-games/{league_id}/{tournament_slug}.csv"):
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
                                    "team_100_blue_id": team_blue,
                                    "team_100_blue_name": team_blue_name,
                                    "team_200_red_id": team_red,
                                    "team_200_red_name": team_red_name,
                                    "game_winner": game_winner,
                                }

                                game_info_event_data = get_game_event_data(retrieved_game_data)
                                all_game_info_data = dict(base_game_info, **game_info_event_data)
                                game_df = pd.DataFrame([all_game_info_data])
                                tournament_games_df_list.append(game_df)

        if not os.path.exists(f"{CREATED_DATA_DIR}/aggregate-games/{league_id}"):
            os.makedirs(f"{CREATED_DATA_DIR}/aggregate-games/{league_id}")
        tournament_df = pd.concat(tournament_games_df_list, ignore_index=True)
        tournament_df.to_csv(f"{CREATED_DATA_DIR}/aggregate-games/{league_id}/{tournament_slug}.csv", index=False)


if __name__ == "__main__":
    # game https://www.youtube.com/watch?v=gapSIdUT8Us
    tournament_to_slug_mapping = get_tournament_to_stage_slug_mapping()
    print(len(tournament_to_slug_mapping))
    team_id_to_info = get_team_id_to_info_mapping()
    print(len(team_id_to_info))
    aggregate_game_data(by_tournament_id="110733838935136200")  # LCS Challengers - only 2 tournaments, good to test
