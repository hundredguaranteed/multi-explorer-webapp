from __future__ import annotations

import csv
import io
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
WORKBOOK_DIR = ROOT.parent / "fiba_youth_workbooks"
OUT_PATH = ROOT / "data" / "fiba_all_seasons.js"

API_BASE = "https://digital-api.fiba.basketball/hapi/"
API_HEADERS = {"Ocp-Apim-Subscription-Key": "898cd5e7389140028ecb42943c47eb74"}
API_TIMEOUT = 45

SUPPLEMENTAL_EVENTS = [
    {"season": 2024, "edition_id": 208821, "competition_key": "u18_euro_a", "competition_label": "FIBA U18 EuroBasket"},
    {"season": 2024, "edition_id": 208822, "competition_key": "u18_euro_b", "competition_label": "FIBA U18 EuroBasket Division B"},
    {"season": 2024, "edition_id": 208819, "competition_key": "u20_euro_a", "competition_label": "FIBA U20 EuroBasket"},
    {"season": 2024, "edition_id": 208830, "competition_key": "u20_euro_b", "competition_label": "FIBA U20 EuroBasket Division B"},
    {"season": 2025, "edition_id": 208899, "competition_key": "u16_americup", "competition_label": "FIBA U16 AmeriCup"},
    {"season": 2025, "edition_id": 208916, "competition_key": "u18_euro_a", "competition_label": "FIBA U18 EuroBasket"},
    {"season": 2025, "edition_id": 208920, "competition_key": "u18_euro_b", "competition_label": "FIBA U18 EuroBasket Division B"},
    {"season": 2025, "edition_id": 208531, "competition_key": "u19_world_cup", "competition_label": "FIBA U19 Basketball World Cup"},
    {"season": 2025, "edition_id": 208914, "competition_key": "u20_euro_a", "competition_label": "FIBA U20 EuroBasket"},
    {"season": 2025, "edition_id": 208924, "competition_key": "u20_euro_b", "competition_label": "FIBA U20 EuroBasket Division B"},
]

OUTPUT_COLUMNS = [
    "season",
    "competition_key",
    "competition_label",
    "edition_id",
    "player_id",
    "player_name",
    "team_name",
    "team_code",
    "nationality",
    "pos",
    "dob",
    "height_in",
    "gp",
    "min",
    "mpg",
    "pts",
    "trb",
    "orb",
    "drb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "plus_minus",
    "plus_minus_pg",
    "eff",
    "eff_pg",
    "fgm",
    "fga",
    "fg_pct",
    "2pm",
    "2pa",
    "2p_pct",
    "3pm",
    "3pa",
    "tp_pct",
    "ftm",
    "fta",
    "ft_pct",
    "efg_pct",
    "ts_pct",
    "orb_pct",
    "drb_pct",
    "trb_pct",
    "ast_pct",
    "ast_to",
    "tov_pct",
    "stl_pct",
    "blk_pct",
    "usg_pct",
    "rgm_per",
]


def clean_number(value, digits: int | None = None):
    if value in ("", None) or pd.isna(value):
        return ""
    number = float(value)
    if digits is None:
        rounded = round(number, 3)
        return int(rounded) if float(rounded).is_integer() else rounded
    rounded = round(number, digits)
    return int(rounded) if digits == 0 and float(rounded).is_integer() else rounded


def clean_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def safe_float(value):
    if value is None or value == "" or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value):
    number = safe_float(value)
    return int(number) if number is not None else 0


def derive_season_map(games_df: pd.DataFrame) -> dict[int, int]:
    season_map: dict[int, int] = {}
    for row in games_df.to_dict("records"):
        edition_id = safe_int(row.get("edition_id"))
        if not edition_id:
            continue
        candidates: list[int] = []
        edition_year = safe_int(row.get("edition_year"))
        if edition_year >= 1900:
            candidates.append(edition_year)
        competition_blob = row.get("competition")
        if isinstance(competition_blob, str) and competition_blob.strip():
            try:
                competition = json.loads(competition_blob)
            except json.JSONDecodeError:
                competition = {}
            season = safe_int(competition.get("season"))
            if season >= 1900:
                candidates.append(season)
        if candidates:
            season_map[edition_id] = max(candidates)
    return season_map


def compute_ts(points, fga, fta):
    if None in (points, fga, fta):
        return ""
    denom = 2 * (float(fga) + (0.44 * float(fta)))
    if denom <= 0:
        return 0
    return round((float(points) / denom) * 100, 1)


def compute_efg(fgm, three_pm, fga):
    if fga is None:
        return ""
    if fga <= 0:
        return 0
    if fgm is None:
        return ""
    return round(((float(fgm) + (0.5 * float(three_pm or 0))) / float(fga)) * 100, 1)


def ratio_raw(numerator, denominator, digits=2):
    if numerator is None or denominator is None or denominator <= 0:
        return ""
    return round(float(numerator) / float(denominator), digits)


def estimate_possessions(fga, fta, tov):
    if None in (fga, fta, tov):
        return None
    return float(fga) + (0.44 * float(fta)) + float(tov)


def estimate_rebound_adjusted_possessions(fga, fta, tov, orb):
    if None in (fga, fta, tov):
        return None
    return float(fga) + (0.44 * float(fta)) - float(orb or 0) + float(tov)


def init_team_totals():
    return {
        "games": 0.0,
        "minutes": 0.0,
        "pts": 0.0,
        "fgm": 0.0,
        "fga": 0.0,
        "2pm": 0.0,
        "2pa": 0.0,
        "3pm": 0.0,
        "3pa": 0.0,
        "ftm": 0.0,
        "fta": 0.0,
        "orb": 0.0,
        "drb": 0.0,
        "trb": 0.0,
        "ast": 0.0,
        "pf": 0.0,
        "tov": 0.0,
        "stl": 0.0,
        "blk": 0.0,
        "eff": 0.0,
        "plus_minus": 0.0,
    }


def add_team_totals(bucket: dict[str, float], row: dict[str, object]) -> None:
    bucket["games"] += 1.0
    bucket["minutes"] += safe_float(row.get("minutes_played_sum")) or 0.0
    bucket["pts"] += safe_float(row.get("points_sum")) or 0.0
    bucket["fgm"] += safe_float(row.get("field_goals_made_sum")) or 0.0
    bucket["fga"] += safe_float(row.get("field_goals_attempted_sum")) or 0.0
    bucket["2pm"] += safe_float(row.get("two_points_made_sum")) or 0.0
    bucket["2pa"] += safe_float(row.get("two_points_attempted_sum")) or 0.0
    bucket["3pm"] += safe_float(row.get("three_points_made_sum")) or 0.0
    bucket["3pa"] += safe_float(row.get("three_points_attempted_sum")) or 0.0
    bucket["ftm"] += safe_float(row.get("free_throws_made_sum")) or 0.0
    bucket["fta"] += safe_float(row.get("free_throws_attempted_sum")) or 0.0
    bucket["orb"] += safe_float(row.get("offensive_rebounds_sum")) or 0.0
    bucket["drb"] += safe_float(row.get("defensive_rebounds_sum")) or 0.0
    bucket["trb"] += safe_float(row.get("rebounds_sum")) or 0.0
    bucket["ast"] += safe_float(row.get("assists_sum")) or 0.0
    bucket["pf"] += safe_float(row.get("personal_fouls_sum")) or 0.0
    bucket["tov"] += safe_float(row.get("turnovers_sum")) or 0.0
    bucket["stl"] += safe_float(row.get("steals_sum")) or 0.0
    bucket["blk"] += safe_float(row.get("blocked_shots_sum")) or 0.0
    bucket["eff"] += safe_float(row.get("efficiency_sum")) or 0.0
    bucket["plus_minus"] += safe_float(row.get("player_plus_minus_sum")) or 0.0


def merge_totals(bucket: dict[str, float], totals: dict[str, float]) -> None:
    for key, value in totals.items():
        bucket[key] = bucket.get(key, 0.0) + (safe_float(value) or 0.0)


def build_team_context(team_game_rows: list[dict[str, object]]) -> dict[tuple[int, int], dict[str, dict[str, float]]]:
    own_totals: dict[tuple[int, int], dict[str, float]] = {}
    opp_totals: dict[tuple[int, int], dict[str, float]] = {}
    for row in team_game_rows:
        edition_id = safe_int(row.get("edition_id"))
        team_id = safe_int(row.get("team_id"))
        opponent_team_id = safe_int(row.get("opponent_team_id"))
        if not edition_id or not team_id:
            continue
        add_team_totals(own_totals.setdefault((edition_id, team_id), init_team_totals()), row)
        if opponent_team_id:
            add_team_totals(opp_totals.setdefault((edition_id, opponent_team_id), init_team_totals()), row)
    keys = set(own_totals) | set(opp_totals)
    return {
        key: {
            "team": own_totals.get(key, init_team_totals()),
            "opp": opp_totals.get(key, init_team_totals()),
        }
        for key in keys
    }


def build_edition_context(team_context: dict[tuple[int, int], dict[str, dict[str, float]]]) -> dict[int, dict[str, float]]:
    editions: dict[int, dict[str, float]] = {}
    for (edition_id, _team_id), context in team_context.items():
        if not edition_id:
            continue
        merge_totals(editions.setdefault(edition_id, init_team_totals()), context.get("team", {}))
    return editions


def compute_advanced_rates(record: dict[str, object], team_context: dict[tuple[int, int], dict[str, dict[str, float]]]) -> dict[str, object]:
    key = (safe_int(record.get("edition_id")), safe_int(record.get("team_id")))
    context = team_context.get(key, {})
    team = context.get("team", init_team_totals())
    opp = context.get("opp", init_team_totals())

    minutes = safe_float(record.get("min"))
    team_minutes = safe_float(team.get("minutes"))
    if minutes is None or team_minutes is None or minutes <= 0 or team_minutes <= 0:
        return {field: "" for field in ("orb_pct", "drb_pct", "trb_pct", "ast_pct", "tov_pct", "stl_pct", "blk_pct", "usg_pct")}

    player_factor = team_minutes / 5
    player_fgm = safe_float(record.get("fgm"))
    player_fga = safe_float(record.get("fga"))
    player_fta = safe_float(record.get("fta"))
    player_tov = safe_float(record.get("tov"))

    team_poss = estimate_possessions(team.get("fga"), team.get("fta"), team.get("tov"))
    opp_poss = estimate_possessions(opp.get("fga"), opp.get("fta"), opp.get("tov"))
    ast_denom = ((minutes / player_factor) * team.get("fgm", 0)) - (player_fgm or 0)
    blk_denom = opp.get("fga", 0) - opp.get("3pa", 0)
    usage_num = None
    if None not in (player_fga, player_fta, player_tov):
        usage_num = player_fga + (0.44 * player_fta) + player_tov
    tov_denom = usage_num

    def bounded(value):
        return clean_number(value, 1) if value != "" else ""

    orb_pct = ""
    if (team.get("orb", 0) + opp.get("drb", 0)) > 0:
        orb_pct = 100 * ((safe_float(record.get("orb")) or 0) * player_factor) / (minutes * (team.get("orb", 0) + opp.get("drb", 0)))

    drb_pct = ""
    if (team.get("drb", 0) + opp.get("orb", 0)) > 0:
        drb_pct = 100 * ((safe_float(record.get("drb")) or 0) * player_factor) / (minutes * (team.get("drb", 0) + opp.get("orb", 0)))

    trb_pct = ""
    if (team.get("trb", 0) + opp.get("trb", 0)) > 0:
        trb_pct = 100 * ((safe_float(record.get("trb")) or 0) * player_factor) / (minutes * (team.get("trb", 0) + opp.get("trb", 0)))

    ast_pct = 100 * (safe_float(record.get("ast")) or 0) / ast_denom if ast_denom > 0 else ""
    tov_pct = 100 * (safe_float(record.get("tov")) or 0) / tov_denom if tov_denom and tov_denom > 0 else ""
    stl_pct = 100 * ((safe_float(record.get("stl")) or 0) * player_factor) / (minutes * opp_poss) if opp_poss and opp_poss > 0 else ""
    blk_pct = 100 * ((safe_float(record.get("blk")) or 0) * player_factor) / (minutes * blk_denom) if blk_denom > 0 else ""
    usg_pct = 100 * (usage_num * player_factor) / (minutes * team_poss) if usage_num is not None and team_poss and team_poss > 0 else ""

    return {
        "orb_pct": bounded(orb_pct),
        "drb_pct": bounded(drb_pct),
        "trb_pct": bounded(trb_pct),
        "ast_pct": bounded(ast_pct),
        "tov_pct": bounded(tov_pct),
        "stl_pct": bounded(stl_pct),
        "blk_pct": bounded(blk_pct),
        "usg_pct": bounded(usg_pct),
    }


def compute_rgm_per_base(
    record: dict[str, object],
    team_context: dict[tuple[int, int], dict[str, dict[str, float]]],
    edition_context: dict[int, dict[str, float]],
) -> float | None:
    edition_id = safe_int(record.get("edition_id"))
    team_id = safe_int(record.get("team_id"))
    key = (edition_id, team_id)
    context = team_context.get(key, {})
    team = context.get("team", init_team_totals())
    opp = context.get("opp", init_team_totals())
    league = edition_context.get(edition_id, init_team_totals())

    minutes = safe_float(record.get("min"))
    if minutes is None or minutes <= 0:
        return None

    lg_fgm = safe_float(league.get("fgm")) or 0.0
    lg_ftm = safe_float(league.get("ftm")) or 0.0
    lg_ast = safe_float(league.get("ast")) or 0.0
    lg_pts = safe_float(league.get("pts")) or 0.0
    lg_fga = safe_float(league.get("fga")) or 0.0
    lg_orb = safe_float(league.get("orb")) or 0.0
    lg_tov = safe_float(league.get("tov")) or 0.0
    lg_trb = safe_float(league.get("trb")) or 0.0
    lg_pf = safe_float(league.get("pf")) or 0.0
    lg_fta = safe_float(league.get("fta")) or 0.0
    lg_games = safe_float(league.get("games")) or 0.0
    team_fgm = safe_float(team.get("fgm")) or 0.0
    team_ast = safe_float(team.get("ast")) or 0.0
    team_games = safe_float(team.get("games")) or 0.0

    if lg_fgm <= 0 or lg_ftm <= 0 or lg_pts <= 0 or lg_trb <= 0 or lg_pf <= 0 or team_fgm <= 0:
        return None

    vop_denom = lg_fga - lg_orb + lg_tov + (0.44 * lg_fta)
    if vop_denom <= 0:
        return None

    factor_denom = 2 * (lg_fgm / lg_ftm)
    if factor_denom == 0:
        return None

    factor = (2 / 3) - ((0.5 * (lg_ast / lg_fgm)) / factor_denom)
    vop = lg_pts / vop_denom
    drb_pct = (lg_trb - lg_orb) / lg_trb
    team_ast_fg = team_ast / team_fgm if team_fgm > 0 else 0.0

    fg = safe_float(record.get("fgm")) or 0.0
    fga = safe_float(record.get("fga")) or 0.0
    three_pm = safe_float(record.get("3pm")) or 0.0
    ftm = safe_float(record.get("ftm")) or 0.0
    fta = safe_float(record.get("fta")) or 0.0
    orb = safe_float(record.get("orb")) or 0.0
    drb = safe_float(record.get("drb"))
    trb = safe_float(record.get("trb"))
    if trb is None:
        trb = orb + (drb or 0.0)
    ast = safe_float(record.get("ast")) or 0.0
    stl = safe_float(record.get("stl")) or 0.0
    blk = safe_float(record.get("blk")) or 0.0
    tov = safe_float(record.get("tov")) or 0.0
    pf = safe_float(record.get("pf")) or 0.0

    uper = (
        three_pm
        + ((2 / 3) * ast)
        + ((2 - (factor * team_ast_fg)) * fg)
        + (ftm * 0.5 * (1 + (1 - team_ast_fg) + ((2 / 3) * team_ast_fg)))
        - (vop * tov)
        - (vop * drb_pct * (fga - fg))
        - (vop * 0.44 * (0.44 + (0.56 * drb_pct)) * (fta - ftm))
        + (vop * (1 - drb_pct) * (trb - orb))
        + (vop * drb_pct * orb)
        + (vop * stl)
        + (vop * drb_pct * blk)
        - (pf * ((lg_ftm / lg_pf) - (0.44 * (lg_fta / lg_pf) * vop)))
    ) / minutes

    team_poss = estimate_rebound_adjusted_possessions(team.get("fga"), team.get("fta"), team.get("tov"), team.get("orb"))
    opp_poss = estimate_rebound_adjusted_possessions(opp.get("fga"), opp.get("fta"), opp.get("tov"), opp.get("orb"))
    league_poss = estimate_rebound_adjusted_possessions(league.get("fga"), league.get("fta"), league.get("tov"), league.get("orb"))
    team_pace = ((team_poss or 0.0) + (opp_poss or 0.0)) / (2 * team_games) if team_games > 0 else None
    league_pace = (league_poss / lg_games) if league_poss and lg_games > 0 else None
    if team_pace and team_pace > 0 and league_pace and league_pace > 0:
        return uper * (league_pace / team_pace)
    return uper


def finalize_rgm_per(rows: list[dict[str, object]]) -> None:
    buckets: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        edition_id = safe_int(row.get("edition_id"))
        if not edition_id:
            continue
        buckets.setdefault(edition_id, []).append(row)

    for edition_rows in buckets.values():
        weighted_total = 0.0
        total_minutes = 0.0
        for row in edition_rows:
            base = safe_float(row.get("_rgm_per_base"))
            minutes = safe_float(row.get("min"))
            if base is None or minutes is None or minutes <= 0:
                continue
            weighted_total += base * minutes
            total_minutes += minutes
        if total_minutes <= 0:
            continue
        average = weighted_total / total_minutes
        if average == 0:
            continue
        scale = 15 / average
        for row in edition_rows:
            base = safe_float(row.get("_rgm_per_base"))
            row["rgm_per"] = clean_number(base * scale, 1) if base is not None else ""


def blank_all_zero_edition_columns(rows: list[dict[str, object]], columns: list[str]) -> None:
    buckets: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        edition_id = safe_int(row.get("edition_id"))
        if not edition_id:
            continue
        buckets.setdefault(edition_id, []).append(row)

    for edition_rows in buckets.values():
        for column in columns:
            numeric_values = [safe_float(row.get(column)) for row in edition_rows if row.get(column) not in ("", None)]
            numeric_values = [value for value in numeric_values if value is not None]
            if not numeric_values:
                continue
            if all(abs(value) < 1e-9 for value in numeric_values):
                for row in edition_rows:
                    row[column] = ""


def build_output_row(
    record: dict[str, object],
    team_context: dict[tuple[int, int], dict[str, dict[str, float]]],
    edition_context: dict[int, dict[str, float]],
) -> dict[str, object]:
    height_cm = safe_float(record.get("height_cm"))
    height_in = round(height_cm / 2.54) if height_cm is not None else ""
    gp = safe_float(record.get("gp"))
    fgm = safe_float(record.get("fgm"))
    fga = safe_float(record.get("fga"))
    two_pm = safe_float(record.get("2pm"))
    two_pa = safe_float(record.get("2pa"))
    three_pm = safe_float(record.get("3pm"))
    three_pa = safe_float(record.get("3pa"))
    ftm = safe_float(record.get("ftm"))
    fta = safe_float(record.get("fta"))
    pts = safe_float(record.get("pts"))
    ast = safe_float(record.get("ast"))
    tov = safe_float(record.get("tov"))

    rates = compute_advanced_rates(record, team_context)
    rgm_per_base = compute_rgm_per_base(record, team_context, edition_context)

    return {
        "season": record.get("season", ""),
        "competition_key": clean_text(record.get("competition_key")),
        "competition_label": clean_text(record.get("competition_label")),
        "edition_id": clean_number(record.get("edition_id")),
        "player_id": clean_number(record.get("player_id")),
        "player_name": clean_text(record.get("player_name")),
        "team_name": clean_text(record.get("team_name")),
        "team_code": clean_text(record.get("team_code")),
        "nationality": clean_text(record.get("nationality") or record.get("team_code")),
        "pos": clean_text(record.get("pos")),
        "dob": clean_text(record.get("dob")),
        "height_in": height_in,
        "gp": clean_number(gp),
        "min": clean_number(record.get("min"), 1),
        "mpg": clean_number(record.get("mpg"), 1),
        "pts": clean_number(pts),
        "trb": clean_number(record.get("trb")),
        "orb": clean_number(record.get("orb")),
        "drb": clean_number(record.get("drb")),
        "ast": clean_number(ast),
        "stl": clean_number(record.get("stl")),
        "blk": clean_number(record.get("blk")),
        "tov": clean_number(tov),
        "pf": clean_number(record.get("pf")),
        "plus_minus": clean_number(record.get("plus_minus")),
        "plus_minus_pg": clean_number(record.get("plus_minus_pg"), 1),
        "eff": clean_number(record.get("eff")),
        "eff_pg": clean_number(record.get("eff_pg"), 1),
        "fgm": clean_number(fgm),
        "fga": clean_number(fga),
        "fg_pct": clean_number(record.get("fg_pct"), 1),
        "2pm": clean_number(two_pm),
        "2pa": clean_number(two_pa),
        "2p_pct": clean_number(record.get("2p_pct"), 1),
        "3pm": clean_number(three_pm),
        "3pa": clean_number(three_pa),
        "tp_pct": clean_number(record.get("tp_pct"), 1),
        "ftm": clean_number(ftm),
        "fta": clean_number(fta),
        "ft_pct": clean_number(record.get("ft_pct"), 1),
        "efg_pct": clean_number(compute_efg(fgm, three_pm, fga), 1),
        "ts_pct": compute_ts(pts, fga, fta),
        "orb_pct": rates["orb_pct"],
        "drb_pct": rates["drb_pct"],
        "trb_pct": rates["trb_pct"],
        "ast_pct": rates["ast_pct"],
        "ast_to": clean_number(ratio_raw(ast, tov, 2), 2),
        "tov_pct": rates["tov_pct"],
        "stl_pct": rates["stl_pct"],
        "blk_pct": rates["blk_pct"],
        "usg_pct": rates["usg_pct"],
        "rgm_per": "",
        "_rgm_per_base": rgm_per_base,
    }


def load_workbook_source(workbook: Path) -> tuple[list[dict[str, object]], dict[tuple[int, int], dict[str, dict[str, float]]]]:
    xls = pd.ExcelFile(workbook)
    players = xls.parse("players")
    teams = xls.parse("teams")[["team_id", "team_name", "team_code"]].drop_duplicates(subset=["team_id"])
    games = xls.parse("games", usecols=lambda col: col in {"edition_id", "edition_year", "competition"})
    team_game_aggregates = xls.parse("team_game_aggregates")

    season_map = derive_season_map(games)
    team_map = {
        safe_int(row["team_id"]): {
            "team_name": clean_text(row["team_name"]),
            "team_code": clean_text(row["team_code"]),
        }
        for row in teams.to_dict("records")
        if safe_int(row.get("team_id"))
    }

    records: list[dict[str, object]] = []
    for row in players.to_dict("records"):
        edition_id = safe_int(row.get("edition_id"))
        fallback_year = safe_int(row.get("edition_year"))
        season = season_map.get(edition_id) or (fallback_year if fallback_year >= 1900 else "")
        team_id = safe_int(row.get("team_id"))
        team_info = team_map.get(team_id, {})
        total_seconds = safe_float(row.get("season_total_play_time_in_seconds"))
        per_game_seconds = safe_float(row.get("season_play_time_in_seconds_per_game"))
        records.append(
            {
                "season": season,
                "competition_key": clean_text(row.get("competition_key")),
                "competition_label": clean_text(row.get("competition_label")),
                "edition_id": edition_id,
                "team_id": team_id,
                "team_name": team_info.get("team_name") or clean_text(row.get("nationality")),
                "team_code": team_info.get("team_code") or clean_text(row.get("country_fiba_code")) or clean_text(row.get("nationality")),
                "nationality": clean_text(row.get("country_fiba_code")) or clean_text(row.get("nationality")) or team_info.get("team_code", ""),
                "player_id": safe_int(row.get("player_id")),
                "player_name": clean_text(row.get("full_name")),
                "pos": clean_text(row.get("position")),
                "dob": clean_text(row.get("date_of_birth")),
                "height_cm": safe_float(row.get("height_cm")),
                "gp": safe_float(row.get("season_total_games_played")),
                "min": (total_seconds / 60) if total_seconds is not None else None,
                "mpg": (per_game_seconds / 60) if per_game_seconds is not None else None,
                "pts": safe_float(row.get("season_total_points")),
                "trb": safe_float(row.get("season_total_rebounds")),
                "orb": safe_float(row.get("season_total_rebounds_offensive")),
                "drb": safe_float(row.get("season_total_rebounds_defensive")),
                "ast": safe_float(row.get("season_total_assists")),
                "stl": safe_float(row.get("season_total_steals")),
                "blk": safe_float(row.get("season_total_blocks")),
                "tov": safe_float(row.get("season_total_turnovers")),
                "pf": safe_float(row.get("season_total_fouls")),
                "plus_minus": safe_float(row.get("season_total_plus_minus")),
                "plus_minus_pg": safe_float(row.get("season_plus_minus_per_game")),
                "eff": safe_float(row.get("season_total_efficiency")),
                "eff_pg": safe_float(row.get("season_efficiency_per_game")),
                "fgm": safe_float(row.get("season_total_field_goals_made")),
                "fga": safe_float(row.get("season_total_field_goals_attempted")),
                "fg_pct": safe_float(row.get("season_field_goals_percentage")),
                "2pm": safe_float(row.get("season_total_two_points_made")),
                "2pa": safe_float(row.get("season_total_two_points_attempted")),
                "2p_pct": safe_float(row.get("season_two_points_percentage")),
                "3pm": safe_float(row.get("season_total_three_points_made")),
                "3pa": safe_float(row.get("season_total_three_points_attempted")),
                "tp_pct": safe_float(row.get("season_three_points_percentage")),
                "ftm": safe_float(row.get("season_total_free_throws_made")),
                "fta": safe_float(row.get("season_total_free_throws_attempted")),
                "ft_pct": safe_float(row.get("season_free_throws_percentage")),
            }
        )

    return records, build_team_context(team_game_aggregates.to_dict("records"))


def fetch_json(path: str):
    last_error = None
    for attempt in range(3):
        try:
            response = requests.get(f"{API_BASE}{path}", headers=API_HEADERS, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(1.5 * (attempt + 1))
    raise last_error


def init_player_record(event: dict[str, object], competition: dict[str, object], bio: dict[str, object], team_info: dict[str, str]) -> dict[str, object]:
    return {
        "season": safe_int(competition.get("season")) or safe_int(event.get("season")) or "",
        "competition_key": event["competition_key"],
        "competition_label": event["competition_label"],
        "edition_id": event["edition_id"],
        "team_id": safe_int(bio.get("team_id")),
        "team_name": team_info.get("team_name", ""),
        "team_code": team_info.get("team_code", ""),
        "nationality": clean_text(bio.get("country_code")) or team_info.get("team_code", ""),
        "player_id": safe_int(bio.get("player_id")),
        "player_name": clean_text(bio.get("player_name")),
        "pos": clean_text(bio.get("pos")),
        "dob": clean_text(bio.get("dob")),
        "height_cm": safe_float(bio.get("height_cm")),
        "gp": 0.0,
        "min": 0.0,
        "mpg": None,
        "pts": 0.0,
        "trb": 0.0,
        "orb": 0.0,
        "drb": 0.0,
        "ast": 0.0,
        "stl": 0.0,
        "blk": 0.0,
        "tov": 0.0,
        "pf": 0.0,
        "plus_minus": 0.0,
        "plus_minus_pg": None,
        "eff": 0.0,
        "eff_pg": None,
        "fgm": 0.0,
        "fga": 0.0,
        "fg_pct": None,
        "2pm": 0.0,
        "2pa": 0.0,
        "2p_pct": None,
        "3pm": 0.0,
        "3pa": 0.0,
        "tp_pct": None,
        "ftm": 0.0,
        "fta": 0.0,
        "ft_pct": None,
    }


def fetch_supplemental_source(event: dict[str, object]) -> tuple[list[dict[str, object]], dict[tuple[int, int], dict[str, dict[str, float]]]]:
    competition = fetch_json(f"getGdapCompetitionById?gdapCompetitionId={event['edition_id']}")
    teams = fetch_json(f"getgdapcompetitionteamsbycompetitionid?gdapCompetitionId={event['edition_id']}&profile=true")
    approved_teams = [team for team in teams if team.get("statusCode") == "APPR"] or teams
    team_map = {
        safe_int(team.get("teamId")): {
            "team_name": clean_text((team.get("profile") or {}).get("name") or team.get("shortName")),
            "team_code": clean_text(team.get("teamCode") or (team.get("profile") or {}).get("code")),
        }
        for team in approved_teams
    }

    roster_payloads: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_json, f"getgdapcompetitionteamlatestrosterbyteamid?gdapTeamId={team_id}"): team_id
            for team_id in team_map
        }
        for future in as_completed(futures):
            roster_payloads.append(future.result())

    bio_by_player: dict[int, dict[str, object]] = {}
    for roster in roster_payloads:
        team_id = safe_int(roster.get("teamId"))
        for player in roster.get("players", []):
            player_id = safe_int(player.get("personId"))
            if not player_id:
                continue
            bio_by_player[player_id] = {
                "player_id": player_id,
                "team_id": team_id,
                "player_name": f"{clean_text(player.get('firstName'))} {clean_text(player.get('lastName'))}".strip(),
                "pos": clean_text(player.get("position")),
                "dob": clean_text(player.get("dateOfBirth")).split("T")[0],
                "height_cm": safe_float(player.get("heightInCm")),
                "country_code": clean_text(player.get("countryFIBACode") or player.get("nationality")),
                "gp": safe_float(player.get("gamesPlayed")),
            }

    team_game_rows: dict[tuple[int, int, int], dict[str, object]] = {}
    records_by_player: dict[int, dict[str, object]] = {
        player_id: init_player_record(event, competition, bio, team_map.get(safe_int(bio.get("team_id")), {}))
        for player_id, bio in bio_by_player.items()
    }

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {
            executor.submit(fetch_json, f"getgdapplayergamestatisticsbyplayerid?gdapPlayerId={player_id}&gdapCompetitionId={event['edition_id']}"): player_id
            for player_id in bio_by_player
        }
        for future in as_completed(futures):
            player_id = futures[future]
            payload = future.result()
            record = records_by_player[player_id]
            game_stats = payload.get("gameStatistics", []) or []
            for game in game_stats:
                record["gp"] += 1
                record["min"] += (safe_float(game.get("playDurationInSeconds")) or 0) / 60
                record["pts"] += safe_float(game.get("points")) or 0
                record["trb"] += safe_float(game.get("rebounds")) or 0
                record["orb"] += safe_float(game.get("offensiveRebounds")) or 0
                record["drb"] += safe_float(game.get("defensiveRebounds")) or 0
                record["ast"] += safe_float(game.get("assists")) or 0
                record["stl"] += safe_float(game.get("steals")) or 0
                record["blk"] += safe_float(game.get("blockedShots")) or 0
                record["tov"] += safe_float(game.get("turnovers")) or 0
                record["pf"] += safe_float(game.get("personalFouls")) or 0
                record["plus_minus"] += safe_float(game.get("plusMinus")) or 0
                record["eff"] += safe_float(game.get("efficiency")) or 0
                record["fgm"] += safe_float(game.get("fieldGoalsMade")) or 0
                record["fga"] += safe_float(game.get("fieldGoalsAttempted")) or 0
                record["2pm"] += safe_float(game.get("twoPointsMade")) or 0
                record["2pa"] += safe_float(game.get("twoPointsAttempted")) or 0
                record["3pm"] += safe_float(game.get("threePointsMade")) or 0
                record["3pa"] += safe_float(game.get("threePointsAttempted")) or 0
                record["ftm"] += safe_float(game.get("freeThrowsMade")) or 0
                record["fta"] += safe_float(game.get("freeThrowsAttempted")) or 0

                team_key = (event["edition_id"], safe_int(game.get("gameId")), safe_int(game.get("teamId")))
                if team_key not in team_game_rows:
                    team_game_rows[team_key] = {
                        "edition_id": event["edition_id"],
                        "game_id": safe_int(game.get("gameId")),
                        "team_id": safe_int(game.get("teamId")),
                        "team_code": clean_text(game.get("teamCode")),
                        "team_name": clean_text(game.get("teamName")),
                        "opponent_team_id": safe_int(game.get("versusTeamId")),
                        "opponent_code": clean_text(game.get("versusTeamCode")),
                        "opponent_name": clean_text(game.get("versusTeamName")),
                        "minutes_played_sum": 0.0,
                        "points_sum": 0.0,
                        "field_goals_made_sum": 0.0,
                        "field_goals_attempted_sum": 0.0,
                        "two_points_made_sum": 0.0,
                        "two_points_attempted_sum": 0.0,
                        "three_points_made_sum": 0.0,
                        "three_points_attempted_sum": 0.0,
                        "free_throws_made_sum": 0.0,
                        "free_throws_attempted_sum": 0.0,
                        "offensive_rebounds_sum": 0.0,
                        "defensive_rebounds_sum": 0.0,
                        "rebounds_sum": 0.0,
                        "assists_sum": 0.0,
                        "personal_fouls_sum": 0.0,
                        "turnovers_sum": 0.0,
                        "steals_sum": 0.0,
                        "blocked_shots_sum": 0.0,
                        "efficiency_sum": 0.0,
                        "player_plus_minus_sum": 0.0,
                    }
                team_row = team_game_rows[team_key]
                team_row["minutes_played_sum"] += (safe_float(game.get("playDurationInSeconds")) or 0) / 60
                team_row["points_sum"] += safe_float(game.get("points")) or 0
                team_row["field_goals_made_sum"] += safe_float(game.get("fieldGoalsMade")) or 0
                team_row["field_goals_attempted_sum"] += safe_float(game.get("fieldGoalsAttempted")) or 0
                team_row["two_points_made_sum"] += safe_float(game.get("twoPointsMade")) or 0
                team_row["two_points_attempted_sum"] += safe_float(game.get("twoPointsAttempted")) or 0
                team_row["three_points_made_sum"] += safe_float(game.get("threePointsMade")) or 0
                team_row["three_points_attempted_sum"] += safe_float(game.get("threePointsAttempted")) or 0
                team_row["free_throws_made_sum"] += safe_float(game.get("freeThrowsMade")) or 0
                team_row["free_throws_attempted_sum"] += safe_float(game.get("freeThrowsAttempted")) or 0
                team_row["offensive_rebounds_sum"] += safe_float(game.get("offensiveRebounds")) or 0
                team_row["defensive_rebounds_sum"] += safe_float(game.get("defensiveRebounds")) or 0
                team_row["rebounds_sum"] += safe_float(game.get("rebounds")) or 0
                team_row["assists_sum"] += safe_float(game.get("assists")) or 0
                team_row["personal_fouls_sum"] += safe_float(game.get("personalFouls")) or 0
                team_row["turnovers_sum"] += safe_float(game.get("turnovers")) or 0
                team_row["steals_sum"] += safe_float(game.get("steals")) or 0
                team_row["blocked_shots_sum"] += safe_float(game.get("blockedShots")) or 0
                team_row["efficiency_sum"] += safe_float(game.get("efficiency")) or 0
                team_row["player_plus_minus_sum"] += safe_float(game.get("plusMinus")) or 0

    records: list[dict[str, object]] = []
    for player_id, record in records_by_player.items():
        gp = record["gp"] or safe_float(bio_by_player[player_id].get("gp")) or 0
        record["gp"] = gp
        record["mpg"] = (record["min"] / gp) if gp > 0 else None
        record["plus_minus_pg"] = (record["plus_minus"] / gp) if gp > 0 else None
        record["eff_pg"] = (record["eff"] / gp) if gp > 0 else None
        record["fg_pct"] = (record["fgm"] / record["fga"] * 100) if record["fga"] > 0 else (0 if record["fga"] == 0 and gp > 0 else None)
        record["2p_pct"] = (record["2pm"] / record["2pa"] * 100) if record["2pa"] > 0 else (0 if record["2pa"] == 0 and gp > 0 else None)
        record["tp_pct"] = (record["3pm"] / record["3pa"] * 100) if record["3pa"] > 0 else (0 if record["3pa"] == 0 and gp > 0 else None)
        record["ft_pct"] = (record["ftm"] / record["fta"] * 100) if record["fta"] > 0 else (0 if record["fta"] == 0 and gp > 0 else None)
        records.append(record)

    return records, build_team_context(list(team_game_rows.values()))


def build_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    for workbook in sorted(WORKBOOK_DIR.glob("*.xlsx")):
        records, team_context = load_workbook_source(workbook)
        edition_context = build_edition_context(team_context)
        rows.extend(build_output_row(record, team_context, edition_context) for record in records)

    existing_keys = {(safe_int(row["season"]), row["competition_key"]) for row in rows}
    for event in SUPPLEMENTAL_EVENTS:
        if (safe_int(event.get("season")), event["competition_key"]) in existing_keys:
            continue
        records, team_context = fetch_supplemental_source(event)
        edition_context = build_edition_context(team_context)
        rows.extend(build_output_row(record, team_context, edition_context) for record in records)

    finalize_rgm_per(rows)
    blank_all_zero_edition_columns(rows, ["plus_minus", "plus_minus_pg", "blk", "blk_pct"])

    rows.sort(
        key=lambda row: (
            int(row["season"]) if row["season"] not in ("", None) else 0,
            clean_text(row["competition_key"]),
            clean_text(row["team_name"]),
            clean_text(row["player_name"]),
        )
    )
    return rows


def write_bundle(rows: list[dict[str, object]]) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=OUTPUT_COLUMNS, lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    csv_text = buffer.getvalue()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(f"window.FIBA_ALL_CSV = {json.dumps(csv_text)};\n", encoding="utf-8")


def main() -> None:
    rows = build_rows()
    write_bundle(rows)
    print(f"Wrote {len(rows)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
