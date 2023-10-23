from enum import Enum

####### Directory consts
CREATED_DATA_DIR = "esports-data/created"
MAPPED_GAMES_DIR = f"{CREATED_DATA_DIR}/mapped-games"
LOL_ESPORTS_DATA_DIR = "esports-data/lol-esports-data"
GAMES_DIR = "games"
TOURNAMENT_TO_SLUGS_MAPPING_PATH = f"{CREATED_DATA_DIR}/tournament_to_stage_slugs_mapping.json"
TEAM_ID_TO_INFO_MAPPING_PATH = f"{CREATED_DATA_DIR}/team_id_to_info_mapping.json"

####### util consts
S3_BUCKET_URL = "https://power-rankings-dataset-gprhack.s3.us-west-2.amazonaws.com/games"

# method consts
STR_SIDE_MAPPING = {"blue": 100, "red": 200}
ROLES = ["top", "jng", "mid", "adc", "sup", "top", "jng", "mid", "adc", "sup"]

####### Feature gathering events and data consts
# Event Types
BUILDING_DESTROYED = "building_destroyed"
CHAMPION_KILL = "champion_kill"
EPIC_MONSTER_KILL = "epic_monster_kill"
STATS_UPDATE = "stats_update"
GAME_INFO = "game_info"

# Kill stats
FIRST_BLOOD = "firstBlood"

# Building Destroyed variables
TURRET = "turret"
INHIBITOR = "inhibitor"
NEXUS = "nexus"

# https://www.youtube.com/watch?v=cH4S7v05Zig - LS Drag Ranks
DRAGON_TYPE_MAPPINGS = {
    "unknown": 0,
    "chemtech": 1,  # chemtech
    "water": 2,  # ocean
    "air": 3,  # cloud
    "fire": 4,  # infernal
    "earth": 5,  # mountain
    "hextech": 6,  # hextech
    "elder": 7,  # elder
}

LANE_MAPPING = {
    "top": 1,
    "mid": 2,
    "bot": 3,
}


# EPIC Monster Kill variables
class Monsters(Enum):
    DRAGON = "dragon"
    BARON = "baron"
    HERALD = "riftHerald"


# Turret types
class Turret(Enum):
    INNER = "inner"
    OUTER = "outer"
    BASE = "base"
    NEXUS = "nexus"


####### Player stats
# Player nested stats
# main level player stats
PARTICIPANT_BASE_INFO = ["summonerName", "championName"]
PARTICIPANT_BASE_INFO_LPL = ["playerName", "championName"]
PARTICIPANT_GENERAL_STATS = ["level", "totalGold"]

PARTICIPANT_GAME_STATS = [
    "MINIONS_KILLED",
    "NEUTRAL_MINIONS_KILLED",
    "NEUTRAL_MINIONS_KILLED_ENEMY_JUNGLE",
    "CHAMPIONS_KILLED",
    "NUM_DEATHS",
    "ASSISTS",
    "WARD_PLACED",
    "WARD_KILLED",
    "VISION_SCORE",
    "TOTAL_DAMAGE_DEALT_TO_CHAMPIONS",
    "TOTAL_DAMAGE_DEALT_TO_BUILDINGS",
    "TOTAL_TIME_CROWD_CONTROL_DEALT_TO_CHAMPIONS",
    "TIME_CCING_OTHERS",
]

#### Team stats
TEAM_STATS = [
    "championsKills",
    "deaths",
    "assists",
    "totalGold",
    "inhibKills",
    "dragonKills",
    "baronKills",
    "towerKills",
]


# mins -> seconds
# JG full clear 1 side, crossing to another side or gank @ 2:30min
# ward used lvl 1 -> 2nd ward at 4 mins (FB potential), jg full clear done at 3:30min
# second rotation of jg camps at 4:20 mins
# first buff taken would come up again at 6:45mins, 2nd one varies based on jg path
# -> full clear 8:15, went directly to buff, 7:15
# another potential kill timer roughly around the 7:30->8:30 min mark
# Level 6 power spike on all lanes: 5:40 (top), 5:30 (mid), 7:40 (bot)
# plate gold is removed at 14mins so herlad into plate gold secure is common before 13:13mins
# since it takes 23 seconds to clear herald (longer if no help), and then walk into lane + herald animations
# Honey fruit spawns -> 6 mins
class ExperienceTimers(Enum):
    FIVE_MINS = 300  # rough FB time + first dragon spawn + blast cones spawn (gank paths)
    TEN_MINS = 600  # rough herald contest time + first dragon has been taken + unleashed teleport
    FIFTEEN_MINS = 900  # mid game


################## FEATURE GATHERING CONSTS ##################
BLUE_CHAMPION_COLUMNS = [
    "1_100_top_championName",
    "2_100_jng_championName",
    "3_100_mid_championName",
    "4_100_adc_championName",
    "5_100_sup_championName",
]

RED_CHAMPION_COLUMNS = [
    "6_200_top_championName",
    "7_200_jng_championName",
    "8_200_mid_championName",
    "9_200_adc_championName",
    "10_200_sup_championName",
]

GOLD_DIFF_BLUE = [
    "100_blue_totalGold_300",
    "100_blue_totalGold_600",
    "100_blue_totalGold_900",
    "100_blue_totalGold_game_end",
]

GOLD_DIFF_RED = [
    "200_red_totalGold_300",
    "200_red_totalGold_600",
    "200_red_totalGold_900",
    "200_red_totalGold_game_end",
]

CLASSIFICATION_CODES = {"Dominant": 1, "Intermediate": 0, "Weaker/Passive": -1}
