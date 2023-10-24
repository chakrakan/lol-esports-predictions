import json
import os
from typing import Optional

import pandas as pd
from constants import (
    BASE_K_VALUE_2023,
    BASE_K_VALUE_MSI_2022,
    BASE_K_VALUE_MSI_2023,
    BASE_K_VALUE_PRE_MSI_2022,
    BASE_K_VALUE_PRE_WORLDS_2022,
    BASE_K_VALUE_WORLDS_2022,
    BLUE_CHAMPION_COLUMNS,
    CREATED_DATA_DIR,
    MAJOR_REGION_MODIFIERS,
    MAPPED_GAMES_DIR,
    RED_CHAMPION_COLUMNS,
    REGION_ELO_MODIFIERS,
)
from feature_utils import get_op_champions

from utils import get_league_tournaments

SORTED_LEAGUE_TOURNAMENTS = f"{CREATED_DATA_DIR}/sorted-tournaments.csv"


def league_id_to_name():
    specified_leagues = ["LPL", "LEC", "LCK", "LCS", "PCS", "TCL", "LCO", "VCS", "CBLOL", "LJL", "LLA", "Worlds", "MSI"]
    leagues_df = pd.read_csv(f"{CREATED_DATA_DIR}/updated-leagues.csv")
    filtered_leagues_df = leagues_df[leagues_df["League Name"].isin(specified_leagues)]

    league_id_to_name_mapping = dict(zip(filtered_leagues_df["League ID"], filtered_leagues_df["League Name"]))
    return league_id_to_name_mapping


def get_team_to_league_mapping():
    if os.path.exists(f"{CREATED_DATA_DIR}/team_name_to_league_mapping.json"):
        with open(f"{CREATED_DATA_DIR}/team_name_to_league_mapping.json", "r") as f:
            team_to_league_mapping = json.load(f)
        return team_to_league_mapping
    league_id_to_teams_mapping = get_league_to_teams_mapping()
    team_to_league_mapping = dict()
    for league_name, teams in league_id_to_teams_mapping.items():
        if league_name not in ["Worlds", "MSI"]:
            for team in teams:
                team_to_league_mapping[team] = league_name
    with open(f"{CREATED_DATA_DIR}/team_name_to_league_mapping.json", "w") as f:
        json.dump(team_to_league_mapping, f)
    return team_to_league_mapping


def get_league_to_teams_mapping():
    if os.path.exists(f"{CREATED_DATA_DIR}/league_id_to_teams_mapping.json"):
        with open(f"{CREATED_DATA_DIR}/league_id_to_teams_mapping.json", "r") as f:
            league_name_to_teams_mapping = json.load(f)
        return league_name_to_teams_mapping

    league_id_to_name_mapping = league_id_to_name()
    league_name_to_teams_mapping = dict()
    for league_id, league_name in league_id_to_name_mapping.items():
        csv_files = [
            (league_id, f)
            for f in os.listdir(f"{MAPPED_GAMES_DIR}/{league_id}")
            if f.endswith(".csv") and "elo" not in f
        ]
        all_unique_teams = set()
        for _, tournament_file in csv_files:
            tournament_df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{league_id}/{tournament_file}")
            all_unique_teams = all_unique_teams | get_unique_team_names(tournament_df)
        league_name_to_teams_mapping[league_name] = list(all_unique_teams)
    with open(f"{CREATED_DATA_DIR}/league_id_to_teams_mapping.json", "w") as f:
        json.dump(league_name_to_teams_mapping, f)
    return league_name_to_teams_mapping


def process_league_ratings(by_date: Optional[str] = None):
    sorted_league_tournaments = pd.read_csv(SORTED_LEAGUE_TOURNAMENTS)
    if by_date:
        sorted_league_tournaments = sorted_league_tournaments[sorted_league_tournaments["game_date"] < by_date]
    sorted_league_tournaments.reset_index(inplace=True, drop=True)

    for index, row in sorted_league_tournaments.iterrows():
        league_id = row["league_id"]
        league_name = league_id_to_name()[league_id]
        print(f"Processing {league_name} - {row['tournament_slug']}...")
        df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{league_id}/{row['tournament_slug']}.csv")
        if index == 0:
            get_tournament_elo(df)
        else:
            prev_tournament = sorted_league_tournaments.iloc[index - 1]["tournament_slug"]
            prev_league_id = sorted_league_tournaments.iloc[index - 1]["league_id"]
            prev_tournament_df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{prev_league_id}/{prev_tournament}.csv")
            last_stage = get_unique_stage_names(prev_tournament_df)[-1]
            existing_elo_df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{prev_league_id}/{prev_tournament}_{last_stage}_elo.csv")
            get_tournament_elo(df, existing_elo_df)
        print(f"{league_name} - {row['tournament_slug']} processed!")


def get_unique_stage_names(tournament_df: pd.DataFrame) -> list:
    return tournament_df["stage_name"].unique().tolist()


def get_unique_team_names(tournament_df: pd.DataFrame) -> set:
    return set(tournament_df["team_100_blue_name"].unique()) | set(tournament_df["team_200_red_name"].unique())


def get_team_name_to_id_mapping():
    if os.path.exists(f"{CREATED_DATA_DIR}/team_name_to_id_mapping.json"):
        with open(f"{CREATED_DATA_DIR}/team_name_to_id_mapping.json", "r") as f:
            reverse_mapping = json.load(f)
        return reverse_mapping

    with open(f"{CREATED_DATA_DIR}/team_id_to_info_mapping.json", "r") as f:
        mapping_data = json.load(f)

    reverse_mapping = {
        team_info["team_name"]: {"ID": team_id, "team_code": team_info["team_code"]}
        for team_id, team_info in mapping_data.items()
    }
    with open(f"{CREATED_DATA_DIR}/team_name_to_id_mapping.json", "w") as f:
        json.dump(reverse_mapping, f)


def get_tournament_elo(tournament_df: pd.DataFrame, existing_elo_df: Optional[pd.DataFrame] = None):
    available_stages = get_unique_stage_names(tournament_df)

    for idx, stage_name in enumerate(available_stages):
        stage_df = tournament_df[tournament_df["stage_name"] == stage_name].copy()
        stage_teams = set(stage_df["team_100_blue_name"].unique()) | set(stage_df["team_200_red_name"].unique())
        if idx == 0 and existing_elo_df is None:
            elo_df = pd.DataFrame(
                {
                    "Team": list(stage_teams),
                    "ELO": [REGION_ELO_MODIFIERS[league_id_to_name()[stage_df.iloc[0]["league_id"]]]]
                    * len(stage_teams),
                }
            )
        elif existing_elo_df is not None:
            elo_df = existing_elo_df
        else:
            elo_df = pd.read_csv(
                f"{MAPPED_GAMES_DIR}/{tournament_df['league_id'][0]}/{tournament_df['tournament_slug'][0]}_{available_stages[idx - 1]}_elo.csv"
            )

        new_teams = stage_teams - set(elo_df["Team"].unique())
        for team in new_teams:
            elo_df.loc[len(elo_df)] = {
                "Team": team,
                "ELO": REGION_ELO_MODIFIERS[get_team_to_league_mapping()[team]],
            }
        stage_df.loc[:, "winning_team"] = stage_df.apply(
            lambda row: row["team_100_blue_name"] if row["game_winner"] == 100 else row["team_200_red_name"], axis=1
        )
        process_tournament_elo(stage_df, elo_df)
        elo_df.sort_values(by=["ELO"], ascending=False, inplace=True)
        print(elo_df)
        elo_df.to_csv(
            f"{MAPPED_GAMES_DIR}/{tournament_df['league_id'][0]}/{tournament_df['tournament_slug'][0]}_{stage_name}_elo.csv",
            index=False,
        )


def get_k_value(game_row: pd.Series):
    # Time specific Base K Values
    BASE_K_VALUE = BASE_K_VALUE_PRE_MSI_2022

    if game_row["game_date"] > MSI_2022_DATE:
        BASE_K_VALUE = BASE_K_VALUE_PRE_WORLDS_2022

    if game_row["game_date"] > WORLDS_2022_DATE:
        BASE_K_VALUE = BASE_K_VALUE_2023

    ### Tournament specific Base K values
    if game_row["tournament_slug"] == "msi_2022":
        BASE_K_VALUE = BASE_K_VALUE_MSI_2022
    if game_row["tournament_slug"] == "worlds_2022":
        BASE_K_VALUE = BASE_K_VALUE_WORLDS_2022
    if game_row["tournament_slug"] == "msi_2023":
        BASE_K_VALUE = BASE_K_VALUE_MSI_2023

    return BASE_K_VALUE


def process_tournament_elo(tournament_df: pd.DataFrame, elo_data: pd.DataFrame):
    for _, row in tournament_df.iterrows():
        winner = row["winning_team"]
        loser = row["team_100_blue_name"] if winner != row["team_100_blue_name"] else row["team_200_red_name"]

        # Weighted K value based on when game was played
        # early 2022 = high K to get initial standings
        # as time goes, lower K values + region modifier to add effect
        # where a team from a weaker region wins against a stronger region, rewarding them better
        k_value = get_k_value(row) * MAJOR_REGION_MODIFIERS[get_team_to_league_mapping()[loser]]

        league_id, tournament_slug, total_games = (
            tournament_df.iloc[0]["league_id"],
            tournament_df.iloc[0]["tournament_slug"],
            tournament_df.shape[0],
        )

        # Feat 1: Increase weight for every OP champion drafted
        op_champions = get_op_champions(
            f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}_champion_mapping.json",
            total_games,
            elo_data["Team"].unique().tolist(),
        )

        # Feat 2: Gold diff end
        gold_diff = abs(row["100_blue_totalGold_game_end"] - row["200_red_totalGold_game_end"])
        if gold_diff > 10000:
            k_value += 8  # gold diff reward
        elif gold_diff > 5000:
            k_value += 4

        if winner == row["team_100_blue_name"]:
            for role in BLUE_CHAMPION_COLUMNS:
                if row[role] in op_champions:
                    k_value += 2

            # Features: team FB, FT, FD, FH, FBaron
            if row["team_first_blood"] == 100:
                k_value += 2
            if row["team_first_turret_destroyed"] == 100:
                k_value += 2
            if row["team_first_dragon_kill"] == 100:
                k_value += 3  # first dragon early adv maintained reward
            if row["team_first_herald_kill"] == 100:
                k_value += 2  # first herald early adv maintained reward
            if row["team_first_baron_kill"] == 100:
                k_value += 4  # first baron early adv maintained reward

            # KD ratio reward for dominance
            total_deaths = 1 if row["100_blue_deaths_game_end"] == 0 else row["100_blue_deaths_game_end"]
            kd_ratio = row["100_blue_championsKills_game_end"] / total_deaths
            if kd_ratio > 1.5:
                k_value += 5
        else:
            k_value += 4  # winning from red side is harder (draft and gameplay wise)
            # bigger reward for winning from red side
            for role in RED_CHAMPION_COLUMNS:
                if row[role] in op_champions:
                    k_value += 2

            if row["team_first_blood"] == 200:
                k_value += 2
            if row["team_first_turret_destroyed"] == 200:
                k_value += 2
            if row["team_first_dragon_kill"] == 200:
                k_value += 3  # first dragon early adv maintained reward
            if row["team_first_herald_kill"] == 200:
                k_value += 2  # first herald early adv maintained reward
            if row["team_first_baron_kill"] == 200:
                k_value += 4  # first baron early adv maintained reward

            total_deaths = 1 if row["200_red_deaths_game_end"] == 0 else row["200_red_deaths_game_end"]
            kd_ratio = row["200_red_championsKills_game_end"] / total_deaths
            if kd_ratio > 1.5:
                k_value += 5

        # Feat 3: Vision score diff
        total_vision_score = abs(row["100_total_VISION_SCORE_game_end"] - row["200_total_VISION_SCORE_game_end"])
        k_value += total_vision_score // 10

        # Feat 4: Total damage to champions diff
        total_dmg_to_champions = abs(
            row["100_total_TOTAL_DAMAGE_DEALT_TO_CHAMPIONS_game_end"]
            - row["200_total_TOTAL_DAMAGE_DEALT_TO_CHAMPIONS_game_end"]
        )
        k_value += total_dmg_to_champions // 10000

        # Feat 5: Game duration - reward shorter more dominant games
        game_duration = row["game_duration"]
        if 1800 > game_duration > 1500:  # 30-25 min
            k_value += 3
        elif game_duration < 1500:  # under 25 min
            k_value += 6

        new_winner_elo, new_loser_elo = update_weighted_elo(
            elo_data.loc[elo_data["Team"] == winner, "ELO"].values[0],
            elo_data.loc[elo_data["Team"] == loser, "ELO"].values[0],
            k_value,
        )

        elo_data.loc[elo_data["Team"] == winner, "ELO"] = new_winner_elo
        elo_data.loc[elo_data["Team"] == loser, "ELO"] = new_loser_elo


def update_weighted_elo(winner_elo: float, loser_elo: float, k_value: int = 30):
    """
    Update the ELO ratings based on the game outcome and game metrics.

    Parameters:
    - winner_elo: ELO rating of the winning team before the game.
    - loser_elo: ELO rating of the losing team before the game.
    - k_value: Base K-factor is 30, weighted based on features
    determines the maximum rating change.

    Returns:
    - Updated ELO ratings for both the winner and the loser.
    """
    # Calculate expected win probabilities
    expected_win_winner = 1 / (1 + 10 ** ((loser_elo - winner_elo) / 400))
    expected_win_loser = 1 / (1 + 10 ** ((winner_elo - loser_elo) / 400))

    # Update ELO based on the outcome and the adjusted k
    new_winner_elo = winner_elo + k_value * (1 - expected_win_winner)
    new_loser_elo = loser_elo + k_value * (0 - expected_win_loser)

    return new_winner_elo, new_loser_elo


if __name__ == "__main__":
    ## Global Event Dates
    MSI_2022_DATE = "2022-05-10"
    WORLDS_2022_DATE = "2022-09-29"
    MSI_2023_DATE = "2023-05-02"

    process_league_ratings()
    # get_team_name_to_id_mapping()
