from enum import Enum

####### Directory consts
CREATED_DATA_DIR = "esports-data/created"
LOL_ESPORTS_DATA_DIR = "esports-data/lol-esports-data"
GAMES_DIR = "league-games"

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

DRAGON_TYPE_MAPPINGS = {
    "fire": 1,  # infernal
    "water": 2,  # ocean
    "earth": 3,  # mountain
    "air": 4,  # cloud
    "hextech": 5,  # hextech
    "chemtech": 6,  # chemtech
    "elder": 7,  #
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
TOTAL_DAMAGE_DEALT_TO_CHAMPIONS = "TOTAL_DAMAGE_DEALT_TO_CHAMPIONS"
TOTAL_TIME_CROWD_CONTROL_DEALT_TO_CHAMPIONS = "TOTAL_TIME_CROWD_CONTROL_DEALT_TO_CHAMPIONS"
CHAMPIONS_KILLED = "CHAMPIONS_KILLED"
NUM_DEATHS = "NUM_DEATHS"
ASSISTS = "ASSISTS"

# player general stats
TOTAL_GOLD = "totalGold"


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
    # Early Game
    FIVE_MINS = 300  # rough FB time + first dragon spawn + blast cones spawn (gank paths)
    TEN_MINS = 600  # rough herald contest time + first dragon has been taken + unleashed teleport
    # Mid Game
    FIFTEEN_MINS = 900
    TWENTY_MINS = 1200  # rough first baron taken
    # End Game
    THIRTY_FIVE_MINS = 2100  # elder dragon spawn -> whichever team gets, rolls the other one
