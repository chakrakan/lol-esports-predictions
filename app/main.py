import pprint
from typing import List, Optional

import pandas as pd
from constants import CREATED_DATA_DIR, MAPPED_GAMES_DIR
from elo import get_team_name_to_id_mapping, get_unique_stage_names, get_unique_team_names

from utils import get_team_id_to_info_mapping


def transform_df_rows_to_response(df: pd.DataFrame):
    resp_array = []
    team_name_mappings = get_team_name_to_id_mapping()
    for index, row in df.iterrows():
        resp = {}
        resp["rank"] = index + 1
        resp["team_name"] = row["Team"]
        resp["team_id"] = team_name_mappings[row["Team"]]["ID"]
        resp["team_code"] = team_name_mappings[row["Team"]]["team_code"]
        resp["ELO"] = row["ELO"]
        resp_array.append(resp)
    return resp_array


def get_tournament_rankings(tournament_id: str, stage: Optional[str] = None):
    tournament_id = int(tournament_id)
    # Load sorted tournaments
    sorted_tournaments = pd.read_csv(f"{CREATED_DATA_DIR}/sorted-tournaments.csv")
    all_available_tournaments = sorted_tournaments["tournament_id"].unique().tolist()
    # Find tournament ID and get league ID and tournament slug
    if tournament_id in all_available_tournaments:
        league_id = sorted_tournaments[sorted_tournaments["tournament_id"] == tournament_id]["league_id"].values[0]
        tournament_slug = sorted_tournaments[sorted_tournaments["tournament_id"] == tournament_id][
            "tournament_slug"
        ].values[0]
        # Load tournament CSV
        tournament_df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{league_id}/{tournament_slug}.csv")
        all_available_stages = get_unique_stage_names(tournament_df)
        if stage and stage in all_available_stages:
            stage_df = tournament_df[tournament_df["stage_name"] == stage].copy()
            elo_df = pd.read_csv(f"{MAPPED_GAMES_DIR}/{league_id}/{tournament_slug}_{stage}_elo.csv")
            # Get unique teams from specific stage
            unique_teams = get_unique_team_names(stage_df)
            filtered_elos = (
                elo_df[elo_df["Team"].isin(unique_teams)]
                .sort_values(by=["ELO"], ascending=False)
                .reset_index(drop=True)
            )
            resp = transform_df_rows_to_response(filtered_elos)
            return resp
        elif stage and stage not in all_available_stages:
            return [{}]

        tournament_elo_file = pd.read_csv(
            f"{MAPPED_GAMES_DIR}/{league_id}/{tournament_slug}_{all_available_stages[-1]}_elo.csv"
        )
        unique_teams = get_unique_team_names(tournament_df)
        filtered_elos = (
            tournament_elo_file[tournament_elo_file["Team"].isin(unique_teams)]
            .sort_values(by=["ELO"], ascending=False)
            .reset_index(drop=True)
        )
        resp = transform_df_rows_to_response(filtered_elos)
        return resp
    return [{}]


def get_global_rankings(number_of_teams: int = 20):
    # Load the last generated ELO file aka LEC Season Finals 2023
    last_generated_elo_file = pd.read_csv(
        f"{MAPPED_GAMES_DIR}/98767991302996019/lec_season_finals_2023_Regional Finals_elo.csv"
    )
    filtered_elos = (
        last_generated_elo_file.sort_values(by=["ELO"], ascending=False).reset_index(drop=True).head(number_of_teams)
    )
    resp = transform_df_rows_to_response(filtered_elos)
    return resp


def get_team_rankings(team_ids: List[str]):
    # Load the last generated ELO file aka LEC Season Finals 2023
    last_generated_elo_file = pd.read_csv(
        f"{MAPPED_GAMES_DIR}/98767991302996019/lec_season_finals_2023_Regional Finals_elo.csv"
    )
    id_to_team_info_mappings = get_team_id_to_info_mapping()
    team_names_from_ids = []
    for team_id in team_ids:
        team_names_from_ids.append(id_to_team_info_mappings[team_id]["team_name"])
    filtered_elos = (
        last_generated_elo_file[last_generated_elo_file["Team"].isin(team_names_from_ids)]
        .sort_values(by=["ELO"], ascending=False)
        .reset_index(drop=True)
    )
    resp = transform_df_rows_to_response(filtered_elos)
    return resp


if __name__ == "__main__":
    pprint.pprint(get_tournament_rankings("107458335260330212", "Groups"))
    pprint.pprint(get_global_rankings())
    pprint.pprint(
        get_team_rankings(["98767991853197861", "99566404852189289", "106972778172351142", "98767991877340524"])
    )
