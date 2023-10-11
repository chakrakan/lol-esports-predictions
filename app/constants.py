####### Directory consts
from enum import Enum

CREATED_DATA_DIR = "esports-data/created"
LOL_ESPORTS_DATA_DIR = "esports-data/lol-esports-data"
GAMES_DIR = "games"

####### util consts
S3_BUCKET_URL = "https://power-rankings-dataset-gprhack.s3.us-west-2.amazonaws.com"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# method consts
STR_SIDE_MAPPING = {"blue": 100, "red": 200}
ROLES = ["top", "jng", "mid", "adc", "sup", "top", "jng", "mid", "adc", "sup"]

####### Feature gathering events and data consts
# Event Types
BUILDING_DESTROYED = "building_destroyed"
CHAMPION_KILL = "champion_kill"
EPIC_MONSTER_KILL = "epic_monster_kill"

# Kill stats
FIRST_BLOOD = "firstBlood"

# Building Destroyed variables
TURRET = "turret"
INHIBITOR = "inhibitor"
NEXUS = "nexus"


# EPIC Monster Kill variables
class Monsters(Enum):
    DRAGON = "dragon"
    BARON = "baron"


# Turret types
class Turret(Enum):
    INNER = "inner"
    OUTER = "outer"


####### Player stats
# Player nested stats
TOTAL_DAMAGE_DEALT_TO_CHAMPIONS = "TOTAL_DAMAGE_DEALT_TO_CHAMPIONS"
TOTAL_TIME_CROWD_CONTROL_DEALT_TO_CHAMPIONS = "TOTAL_TIME_CROWD_CONTROL_DEALT_TO_CHAMPIONS"
CHAMPIONS_KILLED = "CHAMPIONS_KILLED"
NUM_DEATHS = "NUM_DEATHS"
ASSISTS = "ASSISTS"

# player general stats
TOTAL_GOLD = "totalGold"


# mins -> seconds
class ExperienceTimers(Enum):
    FOUR_MINS = 240  # rough FB time
    NINE_MINS = 540  # rough herald contest time
