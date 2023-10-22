import json
import os
from typing import List

import pandas as pd
from constants import BLUE_CHAMPION_COLUMNS, CREATED_DATA_DIR, RED_CHAMPION_COLUMNS


def gather_tournament_features_per_team(directory_path: str, output_file: str):
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
        if f.endswith(".csv")
    ]

    worlds_2022 = ("98767975604431411", "worlds_2022.csv")

    # for league_id, csv_file in csv_files:
    df = pd.read_csv(os.path.join(directory_path, worlds_2022[0], worlds_2022[1]))
    team_feature_extracted_df = extract_team_features(df)
    # team_feature_extracted_df.to_csv(os.path.join(directory_path, league_id, f"TEAM_FE_{csv_file}"), index=False)
    # print(f"Extracted team features for {league_id} - {csv_file}")


def get_all_champion_stats(champion_mapping: dict, unique_teams: List[str]):
    all_champion_stats = {}
    all_cols = [*BLUE_CHAMPION_COLUMNS, *RED_CHAMPION_COLUMNS]
    for role_col in all_cols:
        side = role_col.split("_")[1]
        role = role_col.split("_")[2]
        role_data = champion_mapping[side][role]
        for champ_stats in role_data:
            champion_name = list(champ_stats.keys())[0]
            if champion_name not in all_champion_stats:
                all_champion_stats[champion_name] = {"games_played": 0, "games_won": 0, "teams": set()}

            all_champion_stats[champion_name]["games_played"] += champ_stats[champion_name]
            all_champion_stats[champion_name]["games_won"] += round(
                (champ_stats[champion_name] * champ_stats["winRate"]) / 100
            )

            all_champion_stats[champion_name]["teams"] |= set(unique_teams)

    return all_champion_stats


def get_op_champions(path_to_champion_mapping: str, total_games: int, unique_teams: List[str]):
    """The champion should be picked in more than a certain percentage of all games.
    Threshold to 20% for now.

    High Win Rate: The champion should have a win rate of more than 50%.

    Picked by Multiple Teams: The champion should be picked by more than a certain number of unique teams.
    Threshold to 25% of all unique teams for now.

    Args:
        path_to_champion_mapping (str): _description_
        total_games (int): _description_
    """
    with open(path_to_champion_mapping, "r") as f:
        champion_mapping = json.load(f)

    op_pick_rate_threshold = 0.20 * total_games
    op_team_threshold = 0.25 * len(unique_teams)
    op_criteria_win_rate = 50

    all_champion_stats = get_all_champion_stats(champion_mapping, unique_teams)

    # Determine OP champions
    op_champions = []
    for champ, stats in all_champion_stats.items():
        win_rate = (stats["games_won"] / stats["games_played"]) * 100
        if (
            stats["games_played"] > op_pick_rate_threshold
            and win_rate > op_criteria_win_rate
            and len(stats["teams"]) > op_team_threshold
        ):
            op_champions.append(champ)

    return op_champions


def extract_team_features(df: pd.DataFrame):
    league_id, tournament_slug, total_games = df.iloc[0]["league_id"], df.iloc[0]["tournament_slug"], df.shape[0]
    # get all unique teams from the tournament
    unique_teams = list(set(df["team_100_blue_name"].unique()) | set(df["team_200_red_name"].unique()))

    # get OP champions
    op_champions = get_op_champions(
        f"{CREATED_DATA_DIR}/mapped-games/{league_id}/{tournament_slug}_champion_mapping.json",
        total_games,
        unique_teams,
    )

    # Feature 1: get num times an OP champion has been picked by each team
    all_team_op_picks = {}
    for team in unique_teams:
        team_data_blue = df[df["team_100_blue_name"] == team]
        team_data_red = df[df["team_200_red_name"] == team]

        count_blue = sum([team_data_blue[col].isin(op_champions).sum() for col in BLUE_CHAMPION_COLUMNS])
        count_red = sum([team_data_red[col].isin(op_champions).sum() for col in RED_CHAMPION_COLUMNS])

        all_team_op_picks[team] = count_blue + count_red

    # Feature 2: Overall Win rate of each team
    wins = {team: 0 for team in unique_teams}
    losses = {team: 0 for team in unique_teams}

    for _, row in df.iterrows():
        if row["game_winner"] == 100:
            wins[row["team_100_blue_name"]] += 1
            losses[row["team_200_red_name"]] += 1
        else:
            wins[row["team_200_red_name"]] += 1
            losses[row["team_100_blue_name"]] += 1

    feature_df = pd.DataFrame(
        {
            "Team": unique_teams,
            "Wins": [wins[team] for team in unique_teams],
            "Losses": [losses[team] for team in unique_teams],
            "Total Games Played": [wins[team] + losses[team] for team in unique_teams],
            "Win Rate": [(wins[team] / (wins[team] + losses[team])) * 100 for team in unique_teams],
            "Num OP Champ Picks": [all_team_op_picks[team] for team in unique_teams],
        }
    )
    # feature_df = feature_df.set_index("Team")

    # Feature 3: Avg. Gold differential for each team per time stamp
    df["gold_diff_300"] = df["100_blue_totalGold_300"] - df["200_red_totalGold_300"]
    df["gold_diff_600"] = df["100_blue_totalGold_600"] - df["200_red_totalGold_600"]
    df["gold_diff_900"] = df["100_blue_totalGold_900"] - df["200_red_totalGold_900"]
    df["gold_diff_end"] = df["100_blue_totalGold_game_end"] - df["200_red_totalGold_game_end"]

    blue_gold_diff_300 = df.groupby("team_100_blue_name")["gold_diff_300"].mean()
    red_gold_diff_300 = -df.groupby("team_200_red_name")["gold_diff_300"].mean()

    gold_diff_300 = (blue_gold_diff_300.add(red_gold_diff_300, fill_value=0)) / 2

    blue_gold_diff_600 = df.groupby("team_100_blue_name")["gold_diff_600"].mean()
    red_gold_diff_600 = -df.groupby("team_200_red_name")["gold_diff_600"].mean()

    gold_diff_600 = (blue_gold_diff_600.add(red_gold_diff_600, fill_value=0)) / 2

    blue_gold_diff_900 = df.groupby("team_100_blue_name")["gold_diff_900"].mean()
    red_gold_diff_900 = -df.groupby("team_200_red_name")["gold_diff_900"].mean()

    gold_diff_900 = (blue_gold_diff_900.add(red_gold_diff_900, fill_value=0)) / 2

    blue_gold_diff_end = df.groupby("team_100_blue_name")["gold_diff_end"].mean()
    red_gold_diff_end = -df.groupby("team_200_red_name")["gold_diff_end"].mean()

    gold_diff_end = (blue_gold_diff_end.add(red_gold_diff_end, fill_value=0)) / 2

    feature_df["Avg Gold Diff 300"] = feature_df["Team"].map(gold_diff_300)
    feature_df["Avg Gold Diff 600"] = feature_df["Team"].map(gold_diff_600)
    feature_df["Avg Gold Diff 900"] = feature_df["Team"].map(gold_diff_900)
    feature_df["Avg Gold Diff End"] = feature_df["Team"].map(gold_diff_end)

    # Feature 4: Avg. KDA for each team per time stamp
    print(feature_df[["Team", "Win Rate"]])


if __name__ == "__main__":
    #### Gather tournament features per team
    gather_tournament_features_per_team(
        directory_path=f"{CREATED_DATA_DIR}/mapped-games",
        output_file=f"{CREATED_DATA_DIR}/mapped-tournament-features-per-team.csv",
    )
