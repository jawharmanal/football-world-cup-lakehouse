"""Download and inspect the StatsBomb World Cup 2022 metadata.

This script uses only the Python standard library. Downloaded files are written
under data/, which is excluded from Git.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.request import Request, urlopen


BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
COMPETITION_ID = 43
SEASON_ID = 106
OUTPUT_DIR = Path("data/source")


def download_json(url: str) -> list[dict]:
    request = Request(url, headers={"User-Agent": "football-lakehouse-portfolio"})
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def save_json(data: list[dict], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> None:
    competitions = download_json(f"{BASE_URL}/competitions.json")
    selected = [
        row
        for row in competitions
        if row["competition_id"] == COMPETITION_ID
        and row["season_id"] == SEASON_ID
    ]
    if len(selected) != 1:
        raise RuntimeError("World Cup 2022 competition metadata was not found uniquely.")

    matches = download_json(
        f"{BASE_URL}/matches/{COMPETITION_ID}/{SEASON_ID}.json"
    )
    save_json(competitions, OUTPUT_DIR / "competitions.json")
    save_json(matches, OUTPUT_DIR / "matches_43_106.json")

    teams = {
        match[side]["home_team_name" if side == "home_team" else "away_team_name"]
        for match in matches
        for side in ("home_team", "away_team")
    }
    match_ids = [match["match_id"] for match in matches]

    print("Dataset validated")
    print(f"Competition: {selected[0]['competition_name']}")
    print(f"Season: {selected[0]['season_name']}")
    print(f"Matches: {len(matches)}")
    print(f"Teams: {len(teams)}")
    print(f"Unique match IDs: {len(set(match_ids))}")
    print(f"Output directory: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
