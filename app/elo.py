import os
from typing import Optional

import pandas as pd
from constants import BLUE_CHAMPION_COLUMNS, CREATED_DATA_DIR, MAPPED_GAMES_DIR, RED_CHAMPION_COLUMNS
from feature_utils import get_op_champions

SORTED_LEAGUE_TOURNAMENTS = f"{CREATED_DATA_DIR}/sorted-tournaments.csv"


def league_id_to_name():
    specified_leagues = ["LPL", "LEC", "LCK", "LCS", "PCS", "VCS", "CBLOL", "LJL", "LLA", "Worlds", "MSI"]
    leagues_df = pd.read_csv(f"{CREATED_DATA_DIR}/updated-leagues.csv")
    filtered_leagues_df = leagues_df[leagues_df["League Name"].isin(specified_leagues)]

    league_id_to_name_mapping = dict(zip(filtered_leagues_df["League ID"], filtered_leagues_df["League Name"]))
    return league_id_to_name_mapping


def process_league_ratings(league_id: int):
    sorted_league_tournaments = pd.read_csv(SORTED_LEAGUE_TOURNAMENTS)
    league_name = league_id_to_name()[league_id]
    all_tournaments_in_order = sorted_league_tournaments[sorted_league_tournaments["league_id"] == league_id]
    all_tournaments_in_order.reset_index(inplace=True, drop=True)

    for index, row in all_tournaments_in_order.iterrows():
        print(f"Processing {league_name} - {row['tournament_slug']}...")
        df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{league_id}/{row['tournament_slug']}.csv")
        last_stage = get_unique_stage_names(df)[-1]
        if index == 0:
            get_tournament_elo(df)
        else:
            prev_tournament = all_tournaments_in_order.iloc[index - 1]["tournament_slug"]
            existing_elo_df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{league_id}/{prev_tournament}_{last_stage}_elo.csv")
            get_tournament_elo(df, existing_elo_df)
        print(f"{league_name} - {row['tournament_slug']} processed!")


def get_unique_stage_names(tournament_df: pd.DataFrame) -> list:
    return tournament_df["stage_name"].unique().tolist()


def get_tournament_elo(tournament_df: pd.DataFrame, existing_elo_df: Optional[pd.DataFrame] = None):
    available_stages = get_unique_stage_names(tournament_df)

    for idx, stage_name in enumerate(available_stages):
        stage_df = tournament_df[tournament_df["stage_name"] == stage_name].copy()
        stage_teams = set(stage_df["team_100_blue_name"].unique()) | set(stage_df["team_200_red_name"].unique())
        if idx == 0 and existing_elo_df is None:
            elo_df = pd.DataFrame(
                {
                    "Team": list(stage_teams),
                    "ELO": [1500] * len(stage_teams),
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
            elo_df.loc[len(elo_df)] = {"Team": team, "ELO": 1500}
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


def process_tournament_elo(tournament_df: pd.DataFrame, elo_data: pd.DataFrame):
    for _, row in tournament_df.iterrows():
        k_value = 30
        winner = row["winning_team"]
        loser = row["team_100_blue_name"] if winner != row["team_100_blue_name"] else row["team_200_red_name"]

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
                    k_value += 1

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
        else:
            k_value += 2  # winning from red side is harder (draft and gameplay wise)
            # bigger reward for winning from red side
            for role in RED_CHAMPION_COLUMNS:
                if row[role] in op_champions:
                    k_value += 1

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

        # Feat 3: Game duration
        game_duration = row["game_duration"]
        if 1800 > game_duration > 1500:  # 30-25 min
            k_value += 2  # shorter games reward (more dominance)
        elif game_duration < 1500:  # under 25 min
            k_value += 4

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
    specific_leagues = [
        98767991299243165,  # LCS
        98767991310872058,  # LCK
        98767991314006698,  # LPL
        98767991302996019,  # LEC
        104366947889790212,  # PCS
        107213827295848783,  # VCS
        98767991332355509,  # CBLOL
        98767991349978712,  # LJL
        101382741235120470,  # LLA
        98767991325878492,  # MSI
        98767975604431411,  # Worlds
    ]
    for league_id in specific_leagues:
        process_league_ratings(league_id)
