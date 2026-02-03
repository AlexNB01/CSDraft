import os
import asyncio
import random
import json
import shutil
import datetime
import time
import socket
import struct
import typing
import re
import urllib.request
import urllib.parse
import sqlite3
import math
from types import SimpleNamespace
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv; load_dotenv()

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

# -----------------------------
# Configi :3
# -----------------------------
QUEUE_SIZE = 10
READYCHECK_SECONDS = 120
GUILD_SCOPED = True
PICK_TIMEOUT_SECONDS = 45
MAP_VETO_TIMEOUT_SECONDS = 30
AUTO_VOICE_CHANNELS = True
TEAM1_VOICE_CHANNEL_ID = 1442861436542910494
TEAM2_VOICE_CHANNEL_ID = 1442861481564831785
VOICE_LOBBY_CHANNEL_ID = 364497233061871628
CS2_RCON_HOST = os.getenv("CS2_RCON_HOST", "127.0.0.1")
CS2_RCON_PORT = int(os.getenv("CS2_RCON_PORT", "27015"))
CS2_RCON_PASSWORD = os.getenv("CS2_RCON_PASSWORD", "")
CS2_MATCH_CONFIG_DIR = os.getenv("CS2_MATCH_CONFIG_DIR", "./match_configs")
CS2_MATCH_CONFIG_TARGET_DIR = os.getenv("CS2_MATCH_CONFIG_TARGET_DIR", "")
CS2_MATCH_CONFIG_RCON_DIR = os.getenv("CS2_MATCH_CONFIG_RCON_DIR", "")
CS2_MATCH_CONFIG_URL_BASE = os.getenv("CS2_MATCH_CONFIG_URL_BASE", "")
CS2_MATCH_CONFIG_FORMAT = os.getenv("CS2_MATCH_CONFIG_FORMAT", "matchzy")
CS2_MATCH_CONFIG_EXTRA_JSON = os.getenv("CS2_MATCH_CONFIG_EXTRA_JSON", "")
CS2_MATCH_PLUGIN_START_CMD = os.getenv("CS2_MATCH_PLUGIN_START_CMD", "matchzy_loadmatch")
CS2_SERVER_CONNECT_ADDR = os.getenv("CS2_SERVER_CONNECT_ADDR", "")
CS2_MATCH_RESULTS_DB = os.getenv("CS2_MATCH_RESULTS_DB", "")
CS2_MATCH_RESULTS_POLL_SECONDS = int(os.getenv("CS2_MATCH_RESULTS_POLL_SECONDS", "5"))

# ---- UI: värit ja footer ----
EMBED_COLOR_PRIMARY = 0x29377e
EMBED_FOOTER_TEXT   = "CSDraft by Alex"

PICK_ORDER = [
    "team1", "team2", "team1", "team2", "team1", "team2", "team2"
]
MAP_POOL = [
    "de_ancient",
    "de_anubis",
    "de_dust2",
    "de_inferno",
    "de_mirage",
    "de_nuke",
    "de_overpass",
]

def format_map_name(map_name: str) -> str:
    if not map_name:
        return map_name
    cleaned = map_name
    if cleaned.startswith("de_"):
        cleaned = cleaned[3:]
    cleaned = cleaned.replace("_", " ")
    return cleaned.title()

def format_map_list(map_names: List[str]) -> str:
    if not map_names:
        return "—"
    return ", ".join(format_map_name(name) for name in map_names)

# ---- Elo settings ----
INITIAL_RATING = 1000.0
BASE_MATCH_DELTA = 25.0
MAX_MATCH_DELTA = 30.0
MAX_DRAW_DELTA = 5.0

def build_stats_embed(
    bot_name: str,
    display_name: str,
    games: int, wins: int, winrate: float,
    captain: int, first_picked: int, last_picked: int,
    r_games: int, r_wins: int, r_captain: int, r_first: int, r_last: int,
    total_players: int,
    elo_rating: float,
    r_elo: int,
    avg_pick_round: Optional[float],
) -> discord.Embed:
    emb = discord.Embed(
        title=f"Pelaajatilastot",
        color=EMBED_COLOR_PRIMARY
    )

    emb.add_field(
        name="Pelatut pelit",
        value=f"**{display_name}** on pelannut **{games}** peliä "
              f"({r_games}/{total_players})",
        inline=False
    )
    emb.add_field(
        name="Voitot",
        value=f"**{display_name}** on voittanut **{wins}** peliä (**{winrate:.1f}%** WR) "
              f"({r_wins}/{total_players})",
        inline=False
    )
    emb.add_field(
        name="Elo",
        value=f"**{display_name}** elo: **{int(round(elo_rating))}** "
              f"({r_elo}/{total_players})",
        inline=False
    )

    emb.add_field(
        name="Kapteeni",
        value=f"**{display_name}** on toiminut kapteenina **{captain}** kertaa "
              f"({r_captain}/{total_players})",
        inline=False
    )
    emb.add_field(
        name="Valittu ensimmäisenä",
        value=f"**{display_name}** on valittu ensimmäisenä **{first_picked}** kertaa "
              f"({r_first}/{total_players})",
        inline=False
    )
    emb.add_field(
        name="Valittu viimeisenä",
        value=f"**{display_name}** on valittu viimeisenä **{last_picked}** kertaa "
              f"({r_last}/{total_players})",
        inline=False
    )
    avg_text = f"{avg_pick_round:.2f}" if avg_pick_round is not None else "—"
    emb.add_field(
        name="Valinnan keskiarvo",
        value=f"**{display_name}** on valittu keskimäärin vuorolla **{avg_text}**",
        inline=False,
    )

    emb.set_footer(text="CSDraft by Alex")
    return emb

# -----------------------------
@dataclass
class DraftState:
    queue: List[int] = field(default_factory=list)
    queue_joined_at: Dict[int, datetime.datetime] = field(default_factory=dict)
    readycheck_active: bool = False
    ready_users: Set[int] = field(default_factory=set)
    fake_users: Set[int] = field(default_factory=set)
    ready_task: Optional[asyncio.Task] = None
    draft_active: bool = False
    captains: Tuple[int, int] | None = None
    team1: List[int] = field(default_factory=list)
    team2: List[int] = field(default_factory=list)
    number_by_uid: Dict[int, int] = field(default_factory=dict)
    pick_pool: List[int] = field(default_factory=list)
    pick_order: List[str] = field(default_factory=lambda: PICK_ORDER.copy())
    pick_index: int = 0
    pick_msg: Optional[discord.Message] = None
    pick_timer_task: Optional[asyncio.Task] = None
    pick_deadline_ts: Optional[float] = None
    timer_msg: Optional[discord.Message] = None
    last_pick_prefix: Optional[str] = None
    rc_timer_task: Optional[asyncio.Task] = None
    rc_timer_msg: Optional[discord.Message] = None
    rc_deadline_ts: Optional[float] = None
    pick_timer_seq: int = 0    
    game_id: Optional[int] = None
    map_veto_active: bool = False
    map_pool: List[str] = field(default_factory=list)
    banned_maps: List[str] = field(default_factory=list)
    veto_order: List[str] = field(default_factory=list)
    veto_index: int = 0
    veto_msg: Optional[discord.Message] = None
    veto_timer_task: Optional[asyncio.Task] = None
    veto_deadline_ts: Optional[float] = None
    veto_timer_msg: Optional[discord.Message] = None
    veto_timer_seq: int = 0
    selected_map: Optional[str] = None
    side_selection_active: bool = False
    side_selection_team: Optional[str] = None
    side_selection_msg: Optional[discord.Message] = None
    team1_side: str = "CT"
    team2_side: str = "T"
    result_task: Optional[asyncio.Task] = None


# -----------------------------
# Database tsydeemi :3
# -----------------------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS players (
  user_id INTEGER PRIMARY KEY,
  games_played INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  captain_wins INTEGER NOT NULL DEFAULT 0,
  captain_count INTEGER NOT NULL DEFAULT 0,
  first_pick_count INTEGER NOT NULL DEFAULT 0,
  last_pick_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS games (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guild_id INTEGER NOT NULL,
  team1 TEXT NOT NULL,  -- JSON array of user_ids
  team2 TEXT NOT NULL,  -- JSON array of user_ids
  map TEXT,
  winner INTEGER,       -- 1 or 2, NULL if unset
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ratings (
  user_id TEXT PRIMARY KEY,
  rating REAL NOT NULL,
  elo_games INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rating_history (
  game_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  pre_rating REAL NOT NULL,
  post_rating REAL NOT NULL,
  delta REAL NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(game_id, user_id)
);

CREATE TABLE IF NOT EXISTS captain_opt_out (
  user_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS steam_links (
  discord_user_id INTEGER PRIMARY KEY,
  steamid64 TEXT NOT NULL UNIQUE,
  linked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_player_stats (
  game_id TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  steamid64 TEXT NOT NULL,
  kills INTEGER NOT NULL,
  deaths INTEGER NOT NULL,
  assists INTEGER NOT NULL,
  damage INTEGER NOT NULL,
  rounds INTEGER NOT NULL,
  adr REAL NOT NULL,
  kd REAL NOT NULL,
  rating REAL NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(game_id, user_id)
);
"""

class DB:
    def __init__(self, path: str = "draftbot.sqlite3") -> None:
        self.path = path
        self._lock = asyncio.Lock()

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA_SQL)
            await db.execute("BEGIN")
            async def column_exists(table: str, column: str) -> bool:
                cur = await db.execute(f"PRAGMA table_info({table})")
                rows = await cur.fetchall()
                return any(row[1] == column for row in rows)

            ratings_has_rd = await column_exists("ratings", "rd")
            history_has_pre_rd = await column_exists("rating_history", "pre_rd")
            history_has_post_rd = await column_exists("rating_history", "post_rd")
            games_has_map = await column_exists("games", "map")

            if ratings_has_rd:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ratings_new (
                      user_id TEXT PRIMARY KEY,
                      rating REAL NOT NULL,
                      elo_games INTEGER NOT NULL
                    )
                    """
                )
                await db.execute(
                    "INSERT INTO ratings_new (user_id, rating, elo_games) SELECT user_id, rating, elo_games FROM ratings"
                )
                await db.execute("DROP TABLE ratings")
                await db.execute("ALTER TABLE ratings_new RENAME TO ratings")

            if history_has_pre_rd or history_has_post_rd:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rating_history_new (
                      game_id TEXT NOT NULL,
                      user_id TEXT NOT NULL,
                      pre_rating REAL NOT NULL,
                      post_rating REAL NOT NULL,
                      delta REAL NOT NULL,
                      created_at TEXT NOT NULL,
                      UNIQUE(game_id, user_id)
                    )
                    """
                )
                await db.execute(
                    """
                    INSERT INTO rating_history_new
                      (game_id, user_id, pre_rating, post_rating, delta, created_at)
                    SELECT game_id, user_id, pre_rating, post_rating, delta, created_at
                    FROM rating_history
                    """
                )
                await db.execute("DROP TABLE rating_history")
                await db.execute("ALTER TABLE rating_history_new RENAME TO rating_history")
            try:
                await db.execute("ALTER TABLE players ADD COLUMN captain_wins INTEGER NOT NULL DEFAULT 0")
            except aiosqlite.OperationalError:
                pass
            if not games_has_map:
                try:
                    await db.execute("ALTER TABLE games ADD COLUMN map TEXT")
                except aiosqlite.OperationalError:
                    pass
            await db.commit()

    async def ensure_player(self, user_id: int):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO players (user_id) VALUES (?)",
                    (user_id,),
                )
                await db.commit()

    async def ensure_rating(self, user_id: int):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO ratings (user_id, rating, elo_games) VALUES (?, ?, ?)",
                    (user_id, INITIAL_RATING, 0),
                )
                await db.commit()

    async def upsert_steam_link(self, discord_user_id: int, steamid64: str) -> None:
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    """
                    INSERT INTO steam_links (discord_user_id, steamid64, linked_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(discord_user_id) DO UPDATE SET
                        steamid64 = excluded.steamid64,
                        linked_at = excluded.linked_at
                    """,
                    (discord_user_id, steamid64, timestamp),
                )
                await db.commit()

    async def delete_steam_link(self, discord_user_id: int) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    "DELETE FROM steam_links WHERE discord_user_id = ?",
                    (discord_user_id,),
                )
                await db.commit()

    async def get_steamid(self, discord_user_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT steamid64 FROM steam_links WHERE discord_user_id = ?",
                (discord_user_id,),
            )
            row = await cur.fetchone()
        return row[0] if row else None

    async def get_steamids(self, discord_user_ids: List[int]) -> Dict[int, str]:
        if not discord_user_ids:
            return {}
        placeholders = ",".join("?" for _ in discord_user_ids)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                f"SELECT discord_user_id, steamid64 FROM steam_links WHERE discord_user_id IN ({placeholders})",
                tuple(discord_user_ids),
            )
            rows = await cur.fetchall()
        return {int(uid): str(steamid) for uid, steamid in rows}

    async def is_steamid_taken(self, steamid64: str, except_user_id: Optional[int] = None) -> bool:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT discord_user_id FROM steam_links WHERE steamid64 = ?",
                (steamid64,),
            )
            row = await cur.fetchone()
        if not row:
            return False
        if except_user_id is None:
            return True
        return int(row[0]) != except_user_id

    async def get_rating_rows(self, user_ids: List[int]) -> Dict[int, Tuple[float, int]]:
        if not user_ids:
            return {}
        placeholders = ",".join("?" for _ in user_ids)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                f"SELECT user_id, rating, elo_games FROM ratings WHERE user_id IN ({placeholders})",
                tuple(user_ids),
            )
            rows = await cur.fetchall()
        return {int(uid): (float(rating), int(games)) for uid, rating, games in rows}

    async def get_top_ratings(self, limit: int = 10) -> List[Tuple[int, float, int]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT user_id, rating, elo_games FROM ratings ORDER BY rating DESC, elo_games DESC, user_id ASC LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
        return [(int(uid), float(rating), int(games)) for uid, rating, games in rows]

    async def get_games_played(self, user_ids: List[int]) -> Dict[int, int]:
        if not user_ids:
            return {}
        placeholders = ",".join("?" for _ in user_ids)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                f"SELECT user_id, games_played FROM players WHERE user_id IN ({placeholders})",
                tuple(user_ids),
            )
            rows = await cur.fetchall()
        return {int(uid): int(games) for uid, games in rows}

    async def get_captain_opt_outs(self, user_ids: List[int]) -> Set[int]:
        if not user_ids:
            return set()
        placeholders = ",".join("?" for _ in user_ids)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                f"SELECT user_id FROM captain_opt_out WHERE user_id IN ({placeholders})",
                tuple(user_ids),
            )
            rows = await cur.fetchall()
        return {int(uid) for (uid,) in rows}

    async def set_captain_opt_out(self, user_id: int, opted_out: bool) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                if opted_out:
                    await db.execute(
                        "INSERT OR IGNORE INTO captain_opt_out (user_id) VALUES (?)",
                        (user_id,),
                    )
                else:
                    await db.execute(
                        "DELETE FROM captain_opt_out WHERE user_id = ?",
                        (user_id,),
                    )
                await db.commit()

    async def get_rating_changes_for_game(self, game_id: int) -> Dict[int, float]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT user_id, delta FROM rating_history WHERE game_id = ?",
                (str(game_id),),
            )
            rows = await cur.fetchall()
        return {int(uid): float(delta) for uid, delta in rows}

    async def get_rating_history_for_game(self, game_id: int) -> Dict[int, Tuple[float, float, float]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT user_id, pre_rating, post_rating, delta FROM rating_history WHERE game_id = ?",
                (str(game_id),),
            )
            rows = await cur.fetchall()
        return {
            int(uid): (float(pre_rating), float(post_rating), float(delta))
            for uid, pre_rating, post_rating, delta in rows
        }

    async def upsert_match_player_stats(self, game_id: int, stats: List[dict]) -> None:
        if not stats:
            return
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                for entry in stats:
                    await db.execute(
                        """
                        INSERT INTO match_player_stats
                        (game_id, user_id, steamid64, kills, deaths, assists, damage, rounds, adr, kd, rating, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(game_id, user_id) DO UPDATE SET
                            steamid64 = excluded.steamid64,
                            kills = excluded.kills,
                            deaths = excluded.deaths,
                            assists = excluded.assists,
                            damage = excluded.damage,
                            rounds = excluded.rounds,
                            adr = excluded.adr,
                            kd = excluded.kd,
                            rating = excluded.rating,
                            created_at = excluded.created_at
                        """,
                        (
                            str(game_id),
                            entry["user_id"],
                            entry["steamid64"],
                            entry["kills"],
                            entry["deaths"],
                            entry["assists"],
                            entry["damage"],
                            entry["rounds"],
                            entry["adr"],
                            entry["kd"],
                            entry["rating"],
                            timestamp,
                        ),
                    )
                await db.commit()

    async def get_match_player_stats_for_user(self, user_id: int) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT kills, deaths, assists, damage, rounds, adr, kd, rating
                FROM match_player_stats
                WHERE user_id = ?
                ORDER BY created_at ASC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
        return [
            {
                "kills": int(row[0]),
                "deaths": int(row[1]),
                "assists": int(row[2]),
                "damage": int(row[3]),
                "rounds": int(row[4]),
                "adr": float(row[5]),
                "kd": float(row[6]),
                "rating": float(row[7]),
            }
            for row in rows
        ]

    async def _rollback_ratings_for_game_tx(self, db: aiosqlite.Connection, game_id: int) -> None:
        cur = await db.execute(
            "SELECT user_id, pre_rating FROM rating_history WHERE game_id = ?",
            (str(game_id),),
        )
        rows = await cur.fetchall()
        if not rows:
            return

        for user_id, pre_rating in rows:
            await db.execute(
                "INSERT OR IGNORE INTO ratings (user_id, rating, elo_games) VALUES (?, ?, ?)",
                (user_id, pre_rating, 0),
            )
            await db.execute(
                "UPDATE ratings SET rating = ?, elo_games = CASE WHEN elo_games > 0 THEN elo_games - 1 ELSE 0 END WHERE user_id = ?",
                (pre_rating, user_id),
            )

        await db.execute("DELETE FROM rating_history WHERE game_id = ?", (str(game_id),))

    async def rollback_ratings_for_game(self, game_id: int) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("BEGIN")
                await self._rollback_ratings_for_game_tx(db, game_id)
                await db.commit()

    def _expected_score(self, rating_a: float, rating_b: float) -> float:
        return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / 400.0))

    async def _apply_ratings_for_game_tx(
        self,
        db: aiosqlite.Connection,
        game_id: int,
        team1_ids: List[int],
        team2_ids: List[int],
        result: str,
    ) -> None:
        if result not in {"team1_win", "team2_win", "draw"}:
            raise ValueError("Tuntematon ottelutulos.")

        for uid in team1_ids + team2_ids:
            await db.execute(
                "INSERT OR IGNORE INTO ratings (user_id, rating, elo_games) VALUES (?, ?, ?)",
                (uid, INITIAL_RATING, 0),
            )

        all_ids = team1_ids + team2_ids
        placeholders = ",".join("?" for _ in all_ids)
        cur = await db.execute(
            f"SELECT user_id, rating, elo_games FROM ratings WHERE user_id IN ({placeholders})",
            tuple(all_ids),
        )
        rows = await cur.fetchall()
        ratings_map = {int(uid): (float(rating), int(games)) for uid, rating, games in rows}
        perf_ratings: Dict[int, float] = {}
        try:
            cur = await db.execute(
                f"""
                SELECT user_id, rating
                FROM match_player_stats
                WHERE game_id = ?
                AND user_id IN ({placeholders})
                """,
                (str(game_id), *tuple(all_ids)),
            )
            rows = await cur.fetchall()
            perf_ratings = {int(uid): float(rating) for uid, rating in rows}
        except aiosqlite.Error:
            perf_ratings = {}

        team1_ratings = [ratings_map[uid][0] for uid in team1_ids]
        team2_ratings = [ratings_map[uid][0] for uid in team2_ids]

        team1_rating = average(team1_ratings)
        team2_rating = average(team2_ratings)

        exp_team1 = self._expected_score(team1_rating, team2_rating)
        exp_team2 = 1.0 - exp_team1

        if result == "team1_win":
            score_team1, score_team2 = 1.0, 0.0
        elif result == "team2_win":
            score_team1, score_team2 = 0.0, 1.0
        else:
            score_team1 = score_team2 = 0.5

        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        async def apply_for_team(team_ids: List[int], score_team: float, expected_team: float) -> None:
            for uid in team_ids:
                rating, elo_games = ratings_map[uid]
                delta = BASE_MATCH_DELTA * (score_team - expected_team)
                if score_team == 0.5:
                    delta = _clip(delta, -MAX_DRAW_DELTA, MAX_DRAW_DELTA)
                else:
                    delta = _clip(delta, -MAX_MATCH_DELTA, MAX_MATCH_DELTA)
                perf_rating = perf_ratings.get(uid)
                perf_delta = 0.0
                if perf_rating is not None:
                    perf_delta = _clip(
                        round((perf_rating - 1.0) / 0.20 * 5),
                        -5,
                        5,
                    )
                total_delta = delta + perf_delta
                # Clamp final delta to base max + performance modifier cap.
                max_total = MAX_DRAW_DELTA + 5 if score_team == 0.5 else MAX_MATCH_DELTA + 5
                total_delta = _clip(total_delta, -max_total, max_total)
                new_rating = rating + total_delta

                await db.execute(
                    "UPDATE ratings SET rating = ?, elo_games = elo_games + 1 WHERE user_id = ?",
                    (new_rating, uid),
                )
                await db.execute(
                    """
                    INSERT INTO rating_history
                    (game_id, user_id, pre_rating, post_rating, delta, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(game_id),
                        uid,
                        rating,
                        new_rating,
                        total_delta,
                        timestamp,
                    ),
                )

        await apply_for_team(team1_ids, score_team1, exp_team1)
        await apply_for_team(team2_ids, score_team2, exp_team2)

    async def apply_ratings_for_game(
        self,
        game_id: int,
        team1_ids: List[int],
        team2_ids: List[int],
        result: str,
    ) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("BEGIN")
                await self._apply_ratings_for_game_tx(db, game_id, team1_ids, team2_ids, result)
                await db.commit()

    async def recalc_all_ratings_from_history(self) -> int:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("BEGIN")
                await db.execute("DELETE FROM rating_history")
                await db.execute("DELETE FROM ratings")

                cur = await db.execute(
                    "SELECT id, team1, team2, winner FROM games WHERE winner IS NOT NULL ORDER BY created_at ASC, id ASC"
                )
                games = await cur.fetchall()
                for game_id, team1_raw, team2_raw, winner in games:
                    team1 = json.loads(team1_raw)
                    team2 = json.loads(team2_raw)
                    if winner == 1:
                        result = "team1_win"
                    elif winner == 2:
                        result = "team2_win"
                    else:
                        result = "draw"
                    await self._apply_ratings_for_game_tx(db, game_id, team1, team2, result)

                await db.commit()
                return len(games)

    async def bump_captain(self, user_id: int, delta: int = 1):
        await self.ensure_player(user_id)
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    "UPDATE players SET captain_count = captain_count + ? WHERE user_id = ?",
                    (delta, user_id),
                )
                await db.commit()

    async def bump_first_last(self, user_id: int, first: bool = False, last: bool = False):
        if not (first or last):
            return
        await self.ensure_player(user_id)
        column = "first_pick_count" if first else "last_pick_count"
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    f"UPDATE players SET {column} = {column} + 1 WHERE user_id = ?",
                    (user_id,),
                )
                await db.commit()

    async def record_game(
        self,
        guild_id: int,
        team1: List[int],
        team2: List[int],
        captain1: Optional[int] = None,
        captain2: Optional[int] = None,
    ) -> int:
        if captain1 in team1:
            team1 = [captain1] + [uid for uid in team1 if uid != captain1]
        if captain2 in team2:
            team2 = [captain2] + [uid for uid in team2 if uid != captain2]
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                for uid in team1 + team2:
                    await db.execute("INSERT OR IGNORE INTO players (user_id) VALUES (?)", (uid,))
                    await db.execute(
                        "UPDATE players SET games_played = games_played + 1 WHERE user_id = ?",
                        (uid,),
                    )
                cur = await db.execute(
                    "INSERT INTO games (guild_id, team1, team2) VALUES (?,?,?)",
                    (guild_id, json.dumps(team1), json.dumps(team2)),
                )
                await db.commit()
                return cur.lastrowid

    async def set_game_map(self, game_id: int, map_name: str) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("UPDATE games SET map=? WHERE id=?", (map_name, game_id))
                await db.commit()

    async def get_map_counts(self) -> Dict[str, int]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT map, COUNT(*) FROM games WHERE map IS NOT NULL AND map != '' GROUP BY map"
            )
            rows = await cur.fetchall()
        return {str(map_name): int(count) for map_name, count in rows}

    async def set_winner(self, game_id: int, winner_team: int, overwrite: bool = False) -> Tuple[List[int], List[int]]:
        if winner_team not in (1, 2):
            raise ValueError("Voittajan tulee olla 1 tai 2.")

        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                cur = await db.execute("SELECT team1, team2, winner FROM games WHERE id=?", (game_id,))
                row = await cur.fetchone()
                if not row:
                    raise ValueError("Peliä ei löytynyt tällä ID:llä.")

                team1 = json.loads(row[0])
                team2 = json.loads(row[1])
                previous_winner = row[2]
                captain1 = team1[0] if team1 else None
                captain2 = team2[0] if team2 else None

                if previous_winner is None:
                    await db.execute("UPDATE games SET winner=? WHERE id=?", (winner_team, game_id))
                    winners = team1 if winner_team == 1 else team2
                    for uid in winners:
                        await db.execute("UPDATE players SET wins = wins + 1 WHERE user_id = ?", (uid,))
                    winning_captain = captain1 if winner_team == 1 else captain2
                    if winning_captain is not None:
                        await db.execute(
                            "UPDATE players SET captain_wins = captain_wins + 1 WHERE user_id = ?",
                            (winning_captain,),
                        )
                    result = "team1_win" if winner_team == 1 else "team2_win"
                    await self._apply_ratings_for_game_tx(db, game_id, team1, team2, result)
                    await db.commit()
                    return team1, team2

                if not overwrite:
                    raise ValueError("Tälle pelille on jo asetettu voittaja.")
                    
                if previous_winner == winner_team:
                    await db.commit()
                    return team1, team2

                prev_winners = team1 if previous_winner == 1 else team2
                new_winners  = team1 if winner_team == 1 else team2

                if previous_winner in (1, 2):
                    for uid in prev_winners:
                        await db.execute("UPDATE players SET wins = wins - 1 WHERE user_id = ?", (uid,))
                    prev_captain = captain1 if previous_winner == 1 else captain2
                    if prev_captain is not None:
                        await db.execute(
                            "UPDATE players SET captain_wins = captain_wins - 1 WHERE user_id = ?",
                            (prev_captain,),
                        )
                for uid in new_winners:
                    await db.execute("UPDATE players SET wins = wins + 1 WHERE user_id = ?", (uid,))
                new_captain = captain1 if winner_team == 1 else captain2
                if new_captain is not None:
                    await db.execute(
                        "UPDATE players SET captain_wins = captain_wins + 1 WHERE user_id = ?",
                        (new_captain,),
                    )

                await db.execute("UPDATE games SET winner=? WHERE id=?", (winner_team, game_id))
                await self._rollback_ratings_for_game_tx(db, game_id)
                result = "team1_win" if winner_team == 1 else "team2_win"
                await self._apply_ratings_for_game_tx(db, game_id, team1, team2, result)
                await db.commit()
                return team1, team2

    async def set_draw(self, game_id: int, overwrite: bool = False) -> Tuple[List[int], List[int]]:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                cur = await db.execute("SELECT team1, team2, winner FROM games WHERE id=?", (game_id,))
                row = await cur.fetchone()
                if not row:
                    raise ValueError("Peliä ei löytynyt tällä ID:llä.")

                team1 = json.loads(row[0])
                team2 = json.loads(row[1])
                previous_winner = row[2]
                captain1 = team1[0] if team1 else None
                captain2 = team2[0] if team2 else None

                if previous_winner is None:
                    await db.execute("UPDATE games SET winner=0 WHERE id=?", (game_id,))
                    await self._apply_ratings_for_game_tx(db, game_id, team1, team2, "draw")
                    await db.commit()
                    return team1, team2

                if previous_winner in (1, 2):
                    if not overwrite:
                        raise ValueError("Tälle pelille on jo asetettu voittaja.")
                    prev_winners = team1 if previous_winner == 1 else team2
                    for uid in prev_winners:
                        await db.execute("UPDATE players SET wins = wins - 1 WHERE user_id = ?", (uid,))
                    prev_captain = captain1 if previous_winner == 1 else captain2
                    if prev_captain is not None:
                        await db.execute(
                            "UPDATE players SET captain_wins = captain_wins - 1 WHERE user_id = ?",
                            (prev_captain,),
                        )
                    await db.execute("UPDATE games SET winner=0 WHERE id=?", (game_id,))
                    await self._rollback_ratings_for_game_tx(db, game_id)
                    await self._apply_ratings_for_game_tx(db, game_id, team1, team2, "draw")
                    await db.commit()
                    return team1, team2

                # Oli jo tasapeli (winner=0)
                await db.commit()
                return team1, team2


    async def get_player(self, user_id: int) -> Optional[dict]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT games_played,wins,captain_count,first_pick_count,last_pick_count FROM players WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "games_played": row[0],
                "wins": row[1],
                "captain_count": row[2],
                "first_pick_count": row[3],
                "last_pick_count": row[4],
            }

    async def leaderboard(self, column: str, limit: int = 10) -> List[Tuple[int, int, int]]:
        valid = {
            "games_played": "games_played",
            "wins": "wins",
            "captain_count": "captain_count",
            "first_pick_count": "first_pick_count",
            "last_pick_count": "last_pick_count",
        }
        if column not in valid:
            raise ValueError("Tuntematon sarake leaderbordiin")
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                f"SELECT user_id, {valid[column]}, games_played, wins FROM players ORDER BY {valid[column]} DESC, wins DESC, user_id ASC LIMIT ?",
                (limit,),
            )
            return await cur.fetchall()
            
    async def get_game(self, game_id: int) -> Optional[dict]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT id, guild_id, team1, team2, winner FROM games WHERE id=?",
                (game_id,)
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "guild_id": row[1],
                "team1": json.loads(row[2]),
                "team2": json.loads(row[3]),
                "winner": row[4],  # 1 tai 2 tai None
            }
            
    async def get_rank(self, field: str, user_id: int) -> int:
        valid = {"games_played", "wins", "captain_count", "first_pick_count", "last_pick_count"}
        if field not in valid:
            raise ValueError("Tuntematon kenttä rankille")

        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                # Pelaajan arvo
                cur = await db.execute(f"SELECT {field} FROM players WHERE user_id = ?", (user_id,))
                row = await cur.fetchone()
                target = int(row[0]) if row and row[0] is not None else 0

                # Pelaajien kokonaismäärä
                cur = await db.execute("SELECT COUNT(*) FROM players")
                (total_players,) = await cur.fetchone()
                total_players = int(total_players or 0)

                # Nollissa: kaikille viimeinen sijoitus (== total_players)
                if target <= 0:
                    return total_players

                # Muissa arvoissa: dense ranking
                cur = await db.execute(f"SELECT COUNT(*) FROM players WHERE {field} > ?", (target,))
                (higher_count,) = await cur.fetchone()
                return int(higher_count) + 1


    async def get_elo_rank(self, user_id: int) -> int:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                cur = await db.execute("SELECT rating FROM ratings WHERE user_id = ?", (user_id,))
                row = await cur.fetchone()
                target = float(row[0]) if row and row[0] is not None else 0.0

                cur = await db.execute("SELECT COUNT(*) FROM ratings")
                (total_players,) = await cur.fetchone()
                total_players = int(total_players or 0)

                if target <= 0:
                    return total_players

                cur = await db.execute("SELECT COUNT(*) FROM ratings WHERE rating > ?", (target,))
                (higher_count,) = await cur.fetchone()
                return int(higher_count) + 1

    async def count_players(self) -> int:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM players")
            (n,) = await cur.fetchone()
            return int(n or 0)
            
    async def get_recent_game_ids(self, limit: int = 10) -> List[int]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT id FROM games ORDER BY id DESC LIMIT ?", (limit,))
            rows = await cur.fetchall()
            return [r[0] for r in rows]

    async def get_head_to_head(self, user_id: int, opponent_id: int) -> dict:
        if user_id == opponent_id:
            return {"games": 0, "wins": 0, "losses": 0, "draws": 0}

        games = wins = losses = draws = 0
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT team1, team2, winner FROM games WHERE team1 LIKE ? OR team2 LIKE ?",
                (f"%{user_id}%", f"%{user_id}%"),
            )
            rows = await cur.fetchall()

        for team1_raw, team2_raw, winner in rows:
            team1 = json.loads(team1_raw)
            team2 = json.loads(team2_raw)

            if user_id in team1 and opponent_id in team2:
                user_team = 1
            elif user_id in team2 and opponent_id in team1:
                user_team = 2
            else:
                continue

            if winner is None:
                continue

            games += 1
            if winner == 0:
                draws += 1
            elif winner == user_team:
                wins += 1
            else:
                losses += 1

        return {"games": games, "wins": wins, "losses": losses, "draws": draws}

    async def get_head_to_head_summary(self, user_id: int) -> Dict[int, dict]:
        stats: Dict[int, dict] = {}
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT team1, team2, winner FROM games WHERE team1 LIKE ? OR team2 LIKE ?",
                (f"%{user_id}%", f"%{user_id}%"),
            )
            rows = await cur.fetchall()

        for team1_raw, team2_raw, winner in rows:
            team1 = json.loads(team1_raw)
            team2 = json.loads(team2_raw)

            if user_id in team1:
                user_team = 1
                opponents = team2
            elif user_id in team2:
                user_team = 2
                opponents = team1
            else:
                continue

            if winner is None:
                continue

            for opponent_id in opponents:
                entry = stats.setdefault(
                    opponent_id,
                    {"games": 0, "wins": 0, "losses": 0, "draws": 0},
                )
                entry["games"] += 1
                if winner == 0:
                    entry["draws"] += 1
                elif winner == user_team:
                    entry["wins"] += 1
                else:
                    entry["losses"] += 1

        return stats

    async def get_draws_for_users(self, user_ids: List[int]) -> Dict[int, int]:
        if not user_ids:
            return {}
        target_ids = set(user_ids)
        draws_by_user = {uid: 0 for uid in target_ids}
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT team1, team2 FROM games WHERE winner = 0"
            )
            rows = await cur.fetchall()

        for team1_raw, team2_raw in rows:
            team1 = json.loads(team1_raw)
            team2 = json.loads(team2_raw)
            for uid in team1 + team2:
                if uid in target_ids:
                    draws_by_user[uid] += 1
        return draws_by_user

    async def get_pick_winrates(
        self,
        user_ids: List[int],
        pick_index: int,
    ) -> Dict[int, Dict[str, int]]:
        if not user_ids:
            return {}
        target_ids = set(user_ids)
        stats = {uid: {"games": 0, "wins": 0, "draws": 0} for uid in target_ids}
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT team1, winner FROM games WHERE winner IS NOT NULL"
            )
            rows = await cur.fetchall()

        for team1_raw, winner in rows:
            team1 = json.loads(team1_raw)
            if not team1:
                continue
            if pick_index >= 0 and len(team1) <= pick_index:
                continue
            try:
                picked_uid = team1[pick_index]
            except IndexError:
                continue
            if picked_uid not in target_ids:
                continue
            entry = stats[picked_uid]
            entry["games"] += 1
            if winner == 0:
                entry["draws"] += 1
            elif winner == 1:
                entry["wins"] += 1
        return stats

    async def get_pick_turns_for_user(
        self,
        user_id: int,
        pick_order: List[str],
    ) -> List[int]:
        turns: List[int] = []
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT team1, team2 FROM games"
            )
            rows = await cur.fetchall()

        for team1_raw, team2_raw in rows:
            team1 = json.loads(team1_raw)
            team2 = json.loads(team2_raw)
            if not team1 and not team2:
                continue

            captain1 = team1[0] if team1 else None
            captain2 = team2[0] if team2 else None
            if user_id in {captain1, captain2}:
                continue
            if user_id not in team1 and user_id not in team2:
                continue

            team1_picks = team1[1:] if len(team1) > 1 else []
            team2_picks = team2[1:] if len(team2) > 1 else []
            idx1 = idx2 = 0

            for pick_index, team in enumerate(pick_order, start=1):
                if team == "team1":
                    if idx1 >= len(team1_picks):
                        continue
                    picked_uid = team1_picks[idx1]
                    idx1 += 1
                else:
                    if idx2 >= len(team2_picks):
                        continue
                    picked_uid = team2_picks[idx2]
                    idx2 += 1
                if picked_uid == user_id:
                    turns.append(pick_index)

            leftover = team1_picks[idx1:] + team2_picks[idx2:]
            for offset, picked_uid in enumerate(leftover, start=1):
                if picked_uid == user_id:
                    turns.append(len(pick_order) + offset)

        return turns

# -----------------------------
# Bot :3
# -----------------------------
class DraftBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents, case_insensitive=True)
        self.db = DB()
        self.states: Dict[int, DraftState] = {}  # key: guild_id

    def get_state(self, guild_id: int) -> DraftState:
        if guild_id not in self.states:
            self.states[guild_id] = DraftState()
        return self.states[guild_id]

    async def setup_hook(self) -> None:
        await self.db.init()
        if GUILD_SCOPED and self.guilds:
            for g in self.guilds:
                self.tree.copy_global_to(guild=g)
                await self.tree.sync(guild=g)
        else:
            await self.tree.sync()

bot = DraftBot()

# -----------------------------
# Utility hommelit :3
# -----------------------------

def mention(uid: int) -> str:
    return f"<@{uid}>"

def format_elapsed(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    minutes, sec = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if sec or not parts:
        parts.append(f"{sec}s")
    return " ".join(parts)

def average(values: List[float]) -> float:
    if not values:
        raise ValueError("Average requires at least one value.")
    return sum(values) / len(values)

def average_pick_round(turns: List[int]) -> Optional[float]:
    if not turns:
        return None
    return sum(turns) / len(turns)

def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

async def format_queue_lines(
    interaction: discord.Interaction,
    queue: List[int],
    joined_at_map: Dict[int, datetime.datetime],
) -> List[str]:
    now = datetime.datetime.now(datetime.timezone.utc)
    lines = []
    for uid in queue:
        name = await get_display_name(interaction, uid)
        joined_at = joined_at_map.get(uid)
        if joined_at is None:
            elapsed = "?"
        else:
            elapsed = format_elapsed((now - joined_at).total_seconds())
        lines.append(f"{name} — {elapsed}")
    return lines

async def get_display_name(interaction: discord.Interaction, user_id: int) -> str:
    if interaction.guild:
        m = interaction.guild.get_member(user_id)
        if m and m.display_name:
            return m.display_name
    try:
        u = await bot.fetch_user(user_id)
        return getattr(u, "global_name", None) or u.name
    except Exception:
        return f"User {user_id}"

def _normalize_name(name: str) -> str:
    return name.strip().lower()

async def resolve_user_from_text(
    guild: Optional[discord.Guild],
    text: str
) -> Optional[discord.User]:
    raw = text.strip()
    if not raw:
        return None

    if "#" in raw and guild:
        name_part, _, discrim = raw.rpartition("#")
        if name_part and discrim.isdigit():
            for member in guild.members:
                if member.name == name_part and member.discriminator == discrim:
                    return member

    if raw.isdigit():
        try:
            return await bot.fetch_user(int(raw))
        except Exception:
            return None

    if raw.startswith("<@") and raw.endswith(">"):
        cleaned = raw.strip("<@!>")
        if cleaned.isdigit():
            try:
                return await bot.fetch_user(int(cleaned))
            except Exception:
                return None

    if guild:
        target = _normalize_name(raw)
        exact_matches = [
            member
            for member in guild.members
            if _normalize_name(member.display_name) == target
            or _normalize_name(member.name) == target
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            return None

        partial_matches = [
            member
            for member in guild.members
            if target in _normalize_name(member.display_name)
            or target in _normalize_name(member.name)
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]
    return None

async def show_teams(interaction: discord.Interaction, st: DraftState):
    t1_names = " ".join(mention(u) for u in st.team1)
    t2_names = " ".join(mention(u) for u in st.team2)
    await interaction.followup.send(
        f"**Nykyiset joukkueet**\n**Team 1:** {t1_names if t1_names else '-'}\n**Team 2:** {t2_names if t2_names else '-'}",
        ephemeral=False,
    )

async def get_pick_display_name(interaction: discord.Interaction, st: DraftState, uid: int) -> str:
    return f"test-{uid % 1000000}" if uid in st.fake_users else await get_display_name(interaction, uid)

def _trim_button_label(label: str, max_len: int = 80) -> str:
    if len(label) <= max_len:
        return label
    return f"{label[: max_len - 3]}..."

def is_valid_steamid64(value: str) -> bool:
    return value.isdigit() and len(value) == 17

def mask_steamid64(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}{'*' * (len(value) - 8)}{value[-4:]}"

def remaining_maps(st: DraftState) -> List[str]:
    return [m for m in st.map_pool if m not in st.banned_maps]

def extract_steamid64(text: str) -> Optional[str]:
    text = text.strip()
    if is_valid_steamid64(text):
        return text
    match = re.search(r"(?:/profiles/)(\d{17})", text)
    if match:
        return match.group(1)
    return None

def resolve_vanity_steamid64(url: str) -> Optional[str]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            content = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return None
    match = re.search(r'"steamid":"(\d{17})"', content)
    if match:
        return match.group(1)
    match = re.search(r'"steamid"\s*:\s*"(\d{17})"', content)
    if match:
        return match.group(1)
    return None

class PickButton(discord.ui.Button):
    def __init__(self, uid: int, label: str, row: int):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=label,
            row=row,
            custom_id=f"draft_pick_{uid}",
        )
        self.uid = uid

    async def callback(self, interaction: discord.Interaction):
        view = typing.cast("PickView", self.view)
        await handle_pick_selection(interaction, view.state, self.uid)

class PickView(discord.ui.View):
    def __init__(self, state: DraftState, button_data: List[Tuple[int, str]]):
        super().__init__(timeout=None)
        self.state = state
        for idx, (uid, label) in enumerate(button_data):
            self.add_item(PickButton(uid, _trim_button_label(label), row=idx // 5))

class MapVetoButton(discord.ui.Button):
    def __init__(self, map_name: str, row: int):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=format_map_name(map_name),
            row=row,
            custom_id=f"map_veto_{map_name}",
        )
        self.map_name = map_name

    async def callback(self, interaction: discord.Interaction):
        view = typing.cast("MapVetoView", self.view)
        await handle_veto_selection(interaction, view.state, self.map_name)

class MapVetoView(discord.ui.View):
    def __init__(self, state: DraftState, maps: List[str]):
        super().__init__(timeout=None)
        self.state = state
        for idx, map_name in enumerate(maps):
            self.add_item(MapVetoButton(map_name, row=idx // 5))

class SideSelectButton(discord.ui.Button):
    def __init__(self, side: str, row: int):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=side,
            row=row,
            custom_id=f"side_select_{side}",
        )
        self.side = side

    async def callback(self, interaction: discord.Interaction):
        view = typing.cast("SideSelectView", self.view)
        await handle_side_selection(interaction, view.state, self.side)

class SideSelectView(discord.ui.View):
    def __init__(self, state: DraftState):
        super().__init__(timeout=None)
        self.state = state
        self.add_item(SideSelectButton("CT", row=0))
        self.add_item(SideSelectButton("T", row=0))

async def build_pick_view(interaction: discord.Interaction, st: DraftState) -> discord.ui.View:
    button_data = []
    for uid in st.pick_pool:
        name = await get_pick_display_name(interaction, st, uid)
        button_data.append((uid, name))
    return PickView(st, button_data)

async def build_veto_view(st: DraftState) -> discord.ui.View:
    maps = remaining_maps(st)
    return MapVetoView(st, maps)

async def handle_pick_selection(interaction: discord.Interaction, st: DraftState, uid: int) -> None:
    if not st.draft_active:
        return await interaction.response.send_message("Draft ei ole käynnissä.", ephemeral=True)
    if st.pick_index >= len(st.pick_order):
        return await interaction.response.send_message("Kaikki pelaajat on jo valittu.", ephemeral=True)

    team = st.pick_order[st.pick_index]
    expected_captain = st.captains[0] if team == "team1" else st.captains[1]
    if interaction.user.id != expected_captain:
        return await interaction.response.send_message(
            "Vain vuorossa oleva kapteeni voi valita pelaajan.",
            ephemeral=True,
        )
    if uid not in st.pick_pool:
        return await interaction.response.send_message("Pelaaja on jo valittu.", ephemeral=True)

    await interaction.response.defer(thinking=False)
    await _apply_pick(interaction, st, uid, is_autopick=False)

async def handle_veto_selection(interaction: discord.Interaction, st: DraftState, map_name: str) -> None:
    if not st.map_veto_active:
        return await interaction.response.send_message("Karttaveto ei ole käynnissä.", ephemeral=True)
    if st.selected_map:
        return await interaction.response.send_message("Kartta on jo valittu.", ephemeral=True)
    if st.veto_index >= len(st.veto_order):
        return await interaction.response.send_message("Karttaveto on jo valmis.", ephemeral=True)

    team = st.veto_order[st.veto_index]
    expected_captain = st.captains[0] if team == "team1" else st.captains[1]
    if interaction.user.id != expected_captain:
        return await interaction.response.send_message(
            "Vain vuorossa oleva kapteeni voi bannata kartan.",
            ephemeral=True,
        )
    if map_name not in remaining_maps(st):
        return await interaction.response.send_message("Kartta on jo bannattu.", ephemeral=True)

    await interaction.response.defer(thinking=False)
    await _apply_veto(interaction, st, map_name, is_autoban=False)

async def handle_side_selection(interaction: discord.Interaction, st: DraftState, side: str) -> None:
    if not st.side_selection_active or not st.side_selection_team:
        return await interaction.response.send_message("Puolen valinta ei ole käynnissä.", ephemeral=True)
    if side not in {"CT", "T"}:
        return await interaction.response.send_message("Virheellinen puoli.", ephemeral=True)

    expected_captain = st.captains[0] if st.side_selection_team == "team1" else st.captains[1]
    if interaction.user.id != expected_captain:
        return await interaction.response.send_message(
            "Vain vuorossa oleva kapteeni voi valita puolen.",
            ephemeral=True,
        )

    await interaction.response.defer(thinking=False)
    await _apply_side_selection(interaction, st, side)

async def announce_next_picker(interaction: discord.Interaction, st: DraftState, prefix: Optional[str] = None):
    if st.pick_index >= len(st.pick_order):
        return

    team = st.pick_order[st.pick_index]
    captain = st.captains[0] if team == "team1" else st.captains[1]

    head = prefix if prefix is not None else (st.last_pick_prefix or "")
    st.last_pick_prefix = None
    if head and not head.endswith("\n"):
        head += "\n"

    remaining_block = await build_remaining_block(interaction, st)
    content = (
        f"{head}"
        f"Seuraava vuoro: {mention(captain)}\n\n"
        f"Valitse pelaaja buttoneilla tai komennolla !pick numero\n\n"
        f"{remaining_block}\n"
    )
    view = await build_pick_view(interaction, st)

    if st.pick_msg:
        try:
            st.pick_msg = await st.pick_msg.edit(content=content, view=view)
        except Exception:
            st.pick_msg = await interaction.followup.send(content, view=view, ephemeral=False)
    else:
        st.pick_msg = await interaction.followup.send(content, view=view, ephemeral=False)

    await start_pick_timer(interaction, st)


async def start_pick_timer(interaction: discord.Interaction, st: DraftState):
    st.pick_timer_seq = getattr(st, "pick_timer_seq", 0) + 1
    my_seq = st.pick_timer_seq

    if st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    st.pick_timer_task = None

    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
        st.timer_msg = None

    st.pick_deadline_ts = asyncio.get_running_loop().time() + PICK_TIMEOUT_SECONDS
    text = f"⏳ **{PICK_TIMEOUT_SECONDS}s** aikaa valita…"
    if interaction.channel:
        st.timer_msg = await interaction.channel.send(text)

    st.pick_timer_task = asyncio.create_task(_run_pick_countdown(interaction, st, my_seq))


async def build_remaining_block(interaction: discord.Interaction, st: DraftState) -> str:
    rating_rows = await bot.db.get_rating_rows(st.pick_pool)
    lines = []
    for u in st.pick_pool:
        name = await get_pick_display_name(interaction, st, u)
        num = st.number_by_uid.get(u, "?")
        rating, _games = rating_rows.get(u, (INITIAL_RATING, 0))
        lines.append(f"{num} - {name} ({int(round(rating))})")
    return "```\nValittavissa:\n" + "\n".join(lines) + "\n```" if lines else "```\nValittavissa:\n(ei ketään)\n```"

async def _run_pick_countdown(interaction: discord.Interaction, st: DraftState, seq: int):
    try:
        loop = asyncio.get_running_loop()

        def remaining() -> int:
            now = loop.time()
            return int(max(0, (st.pick_deadline_ts or now) - now))

        recreated_once = False

        while st.draft_active and st.pick_index < len(st.pick_order):
            if getattr(st, "pick_timer_seq", 0) != seq:
                return

            rem = remaining()

            if st.timer_msg:
                try:
                    await st.timer_msg.edit(content=f"⏳ **{rem}s** aikaa valita…")
                except Exception:
                    try:
                        await st.timer_msg.delete()
                    except Exception:
                        pass
                    st.timer_msg = None

            if st.timer_msg is None and not recreated_once and interaction.channel and rem > 0 and st.pick_timer_seq == seq:
                st.timer_msg = await interaction.channel.send(f"⏳ **{rem}s** aikaa valita…")
                recreated_once = True

            if rem <= 0:
                break
            await asyncio.sleep(1)

        if st.draft_active and st.pick_index < len(st.pick_order) and st.pick_pool and getattr(st, "pick_timer_seq", 0) == seq:
            uid = random.choice(st.pick_pool)
            await _apply_pick(interaction, st, uid, is_autopick=True)
            return

    except asyncio.CancelledError:
        return

async def start_veto_timer(interaction: discord.Interaction, st: DraftState):
    st.veto_timer_seq = getattr(st, "veto_timer_seq", 0) + 1
    my_seq = st.veto_timer_seq

    if st.veto_timer_task and not st.veto_timer_task.done():
        st.veto_timer_task.cancel()
    st.veto_timer_task = None

    if st.veto_timer_msg:
        try:
            await st.veto_timer_msg.delete()
        except Exception:
            pass
        st.veto_timer_msg = None

    st.veto_deadline_ts = asyncio.get_running_loop().time() + MAP_VETO_TIMEOUT_SECONDS
    text = f"⏳ **{MAP_VETO_TIMEOUT_SECONDS}s** aikaa bannata kartta…"
    if interaction.channel:
        st.veto_timer_msg = await interaction.channel.send(text)

    st.veto_timer_task = asyncio.create_task(_run_veto_countdown(interaction, st, my_seq))

async def _run_veto_countdown(interaction: discord.Interaction, st: DraftState, seq: int):
    try:
        loop = asyncio.get_running_loop()

        def remaining() -> int:
            now = loop.time()
            return int(max(0, (st.veto_deadline_ts or now) - now))

        recreated_once = False

        while st.map_veto_active and st.veto_index < len(st.veto_order):
            if getattr(st, "veto_timer_seq", 0) != seq:
                return

            rem = remaining()

            if st.veto_timer_msg:
                try:
                    await st.veto_timer_msg.edit(content=f"⏳ **{rem}s** aikaa bannata kartta…")
                except Exception:
                    try:
                        await st.veto_timer_msg.delete()
                    except Exception:
                        pass
                    st.veto_timer_msg = None

            if st.veto_timer_msg is None and not recreated_once and interaction.channel and rem > 0 and st.veto_timer_seq == seq:
                st.veto_timer_msg = await interaction.channel.send(f"⏳ **{rem}s** aikaa bannata kartta…")
                recreated_once = True

            if rem <= 0:
                break
            await asyncio.sleep(1)

        if st.map_veto_active and st.veto_index < len(st.veto_order) and remaining_maps(st) and getattr(st, "veto_timer_seq", 0) == seq:
            map_name = random.choice(remaining_maps(st))
            await _apply_veto(interaction, st, map_name, is_autoban=True)
            return

    except asyncio.CancelledError:
        return



async def _apply_pick(interaction: discord.Interaction, st: DraftState, uid: int, is_autopick: bool = False):
    if st.pick_index >= len(st.pick_order):
        return

    current_team = st.pick_order[st.pick_index]
    target_team = st.team1 if current_team == "team1" else st.team2

    if uid in st.pick_pool:
        target_team.append(uid)
        st.pick_pool.remove(uid)

    if st.pick_index == 0:
        await bot.db.bump_first_last(uid, first=True)

    st.pick_index += 1

    team_num = 1 if current_team == "team1" else 2
    picked_name = await get_pick_display_name(interaction, st, uid)
    st.last_pick_prefix = f"Pelaaja {picked_name} lisätty tiimiin {team_num}."

    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
        st.timer_msg = None

    if not is_autopick and st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    st.pick_timer_task = None

    if st.pick_index < len(st.pick_order):
        st.pick_msg = None
        await _finish_or_next(interaction, st)
        return

    await _finish_or_next(interaction, st)

async def _clear_veto_ui(st: DraftState):
    if st.veto_timer_task and not st.veto_timer_task.done():
        st.veto_timer_task.cancel()
    st.veto_timer_task = None
    if st.veto_timer_msg:
        try:
            await st.veto_timer_msg.delete()
        except Exception:
            pass
        st.veto_timer_msg = None
    if st.veto_msg:
        try:
            await st.veto_msg.edit(view=None)
        except Exception:
            pass
        st.veto_msg = None

async def announce_next_veto(interaction: discord.Interaction, st: DraftState, prefix: Optional[str] = None):
    if not st.map_veto_active or st.veto_index >= len(st.veto_order):
        return

    team = st.veto_order[st.veto_index]
    captain = st.captains[0] if team == "team1" else st.captains[1]
    team_label = "Team 1" if team == "team1" else "Team 2"

    head = prefix or ""
    if head and not head.endswith("\n"):
        head += "\n"

    remaining = remaining_maps(st)
    remaining_text = format_map_list(remaining)
    banned_text = format_map_list(st.banned_maps)
    content = (
        f"{head}"
        f"**Map veto**\n"
        f"Vuoro: {mention(captain)} ({team_label})\n"
        f"Jäljellä: {remaining_text}\n"
        f"Bannatut: {banned_text}\n"
        f"Klikkaa karttaa banataksesi."
    )
    view = await build_veto_view(st)

    if st.veto_msg:
        try:
            st.veto_msg = await st.veto_msg.edit(content=content, view=view)
        except Exception:
            st.veto_msg = await interaction.followup.send(content, view=view, ephemeral=False)
    else:
        st.veto_msg = await interaction.followup.send(content, view=view, ephemeral=False)

    await start_veto_timer(interaction, st)

async def start_map_veto(interaction: discord.Interaction, st: DraftState):
    await _clear_veto_ui(st)
    st.map_veto_active = True
    st.map_pool = MAP_POOL.copy()
    st.banned_maps = []
    st.veto_order = ["team1" if i % 2 == 0 else "team2" for i in range(max(0, len(st.map_pool) - 1))]
    st.veto_index = 0
    st.selected_map = None
    await announce_next_veto(interaction, st)

async def _apply_veto(interaction: discord.Interaction, st: DraftState, map_name: str, is_autoban: bool = False):
    remaining = remaining_maps(st)
    if map_name not in remaining:
        return

    st.banned_maps.append(map_name)
    st.veto_index += 1

    if st.veto_timer_msg:
        try:
            await st.veto_timer_msg.delete()
        except Exception:
            pass
        st.veto_timer_msg = None

    if not is_autoban and st.veto_timer_task and not st.veto_timer_task.done():
        st.veto_timer_task.cancel()
    st.veto_timer_task = None

    remaining = remaining_maps(st)
    if len(remaining) <= 1:
        await _finish_map_veto(interaction, st, remaining[0] if remaining else None)
        return

    map_label = format_map_name(map_name)
    prefix = f"Kartta **{map_label}** bannattu."
    if is_autoban:
        prefix = f"Aikaraja! Kartta **{map_label}** bannattu automaattisesti."
    await announce_next_veto(interaction, st, prefix=prefix)

async def _finish_map_veto(interaction: discord.Interaction, st: DraftState, selected_map: Optional[str]):
    st.map_veto_active = False
    st.selected_map = selected_map
    await _clear_veto_ui(st)

    if not selected_map:
        await interaction.followup.send("Karttaveto epäonnistui: karttaa ei löytynyt.")
        return

    if st.game_id:
        await bot.db.set_game_map(st.game_id, selected_map)

    await interaction.followup.send(f"**Kartta valittu:** {format_map_name(selected_map)}")
    if st.veto_index > 0 and st.captains:
        last_team = st.veto_order[st.veto_index - 1] if st.veto_order else None
        chooser_team = "team1" if last_team == "team2" else "team2"
        await start_side_selection(interaction, st, chooser_team)
        return
    await start_server_orchestration(interaction, st)

async def start_side_selection(interaction: discord.Interaction, st: DraftState, chooser_team: str) -> None:
    st.side_selection_active = True
    st.side_selection_team = chooser_team
    st.team1_side = "CT"
    st.team2_side = "T"

    captain = st.captains[0] if chooser_team == "team1" else st.captains[1]
    team_label = "Team 1" if chooser_team == "team1" else "Team 2"
    content = (
        f"Puolen valinta: {mention(captain)} ({team_label})\n"
        "Valitse puoli napista."
    )
    view = SideSelectView(st)
    if st.side_selection_msg:
        try:
            st.side_selection_msg = await st.side_selection_msg.edit(content=content, view=view)
        except Exception:
            st.side_selection_msg = await interaction.followup.send(content, view=view, ephemeral=False)
    else:
        st.side_selection_msg = await interaction.followup.send(content, view=view, ephemeral=False)

async def _apply_side_selection(interaction: discord.Interaction, st: DraftState, side: str) -> None:
    chooser_team = st.side_selection_team
    if chooser_team == "team1":
        st.team1_side = side
        st.team2_side = "T" if side == "CT" else "CT"
    else:
        st.team2_side = side
        st.team1_side = "T" if side == "CT" else "CT"

    st.side_selection_active = False
    st.side_selection_team = None

    if st.side_selection_msg:
        try:
            await st.side_selection_msg.edit(view=None)
        except Exception:
            pass
        st.side_selection_msg = None

    await interaction.followup.send(
        f"Puoli valittu. Team 1: **{st.team1_side}**, Team 2: **{st.team2_side}**."
    )
    await start_server_orchestration(interaction, st)


async def _finish_or_next(interaction: discord.Interaction, st: DraftState):
    if st.pick_index >= len(st.pick_order):
        if len(st.pick_pool) == 1:
            last_uid = st.pick_pool.pop()
            st.team1.append(last_uid)
            await bot.db.bump_first_last(last_uid, last=True)

        if st.pick_msg:
            try:
                await st.pick_msg.edit(view=None)
            except Exception:
                pass
            st.pick_msg = None

        captain1 = st.captains[0] if st.captains else None
        captain2 = st.captains[1] if st.captains else None
        game_id = await bot.db.record_game(interaction.guild_id, st.team1, st.team2, captain1, captain2)
        st.game_id = game_id

        await backup_db()

        rating_rows = await bot.db.get_rating_rows(st.team1 + st.team2)
        team1_ratings = [
            rating_rows.get(uid, (INITIAL_RATING, 0))[0]
            for uid in st.team1
        ]
        team2_ratings = [
            rating_rows.get(uid, (INITIAL_RATING, 0))[0]
            for uid in st.team2
        ]
        team1_avg = average(team1_ratings) if team1_ratings else INITIAL_RATING
        team2_avg = average(team2_ratings) if team2_ratings else INITIAL_RATING

        names1 = [ (f"test-{u % 1000000}" if u in st.fake_users else await get_display_name(interaction, u)) for u in st.team1 ]
        names2 = [ (f"test-{u % 1000000}" if u in st.fake_users else await get_display_name(interaction, u)) for u in st.team2 ]

        team1_avg_display = int(round(team1_avg))
        team2_avg_display = int(round(team2_avg))

        emb = discord.Embed(title="Valitut joukkueet", color=EMBED_COLOR_PRIMARY)
        emb.add_field(
            name=f"Team1 ({team1_avg_display}):",
            value=("\n".join(names1) if names1 else "-"),
            inline=True,
        )
        emb.add_field(
            name=f"Team2 ({team2_avg_display}):",
            value=("\n".join(names2) if names2 else "-"),
            inline=True,
        )
        emb.set_footer(text="CSDraft by Alex")

        await interaction.followup.send(
            content=(
                f"**Draft valmis!** Pelin ID: `{game_id}`\n"
                f"Aseta voittaja: `!setwinner {game_id} 1` tai `!setwinner {game_id} 2`."
            ),
            embed=emb
        )
        
        team1_ids = list(st.team1)
        team2_ids = list(st.team2)
        fake_users = set(st.fake_users)

        if AUTO_VOICE_CHANNELS:
            countdown_msg = await interaction.followup.send(
                "Pelaajat siirretään voice-kanaville **5s** kuluttua…"
            )
            asyncio.create_task(
                voice_move_countdown(
                    interaction,
                    team1_ids=team1_ids,
                    team2_ids=team2_ids,
                    fake_users=fake_users,
                    msg=countdown_msg,
                )
            )

        drafted = set(st.team1 + st.team2)
        st.queue = [u for u in st.queue if u not in drafted]
        st.queue_joined_at = {
            uid: joined_at
            for uid, joined_at in st.queue_joined_at.items()
            if uid in st.queue
        }
        st.draft_active = False
        st.pick_pool.clear()
        st.pick_index = 0
        st.number_by_uid.clear()
        st.last_pick_prefix = None 
        st.selected_map = None

        if st.pick_timer_task and not st.pick_timer_task.done():
            st.pick_timer_task.cancel()
        st.pick_timer_task = None
        if st.timer_msg:
            try:
                await st.timer_msg.delete()
            except Exception:
                pass
        st.timer_msg = None
        st.pick_msg = None
        await start_map_veto(interaction, st)
        return

    if st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    st.pick_timer_task = None
    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
    st.timer_msg = None

    await announce_next_picker(interaction, st)

async def move_players_to_fixed_voice_channels(
    guild: discord.Guild,
    fake_users: Set[int],
    team1_ids: List[int],
    team2_ids: List[int],
) -> str:
    if not AUTO_VOICE_CHANNELS:
        return "🎧 Voice-siirto ei ole käytössä (AUTO_VOICE_CHANNELS = False)."

    if not TEAM1_VOICE_CHANNEL_ID or not TEAM2_VOICE_CHANNEL_ID:
        return (
            "⚠️ Voice-siirto on päällä, mutta TEAM1_VOICE_CHANNEL_ID / TEAM2_VOICE_CHANNEL_ID "
            "ei ole asetettu configissa."
        )

    ch1 = guild.get_channel(TEAM1_VOICE_CHANNEL_ID)
    ch2 = guild.get_channel(TEAM2_VOICE_CHANNEL_ID)

    if not isinstance(ch1, discord.VoiceChannel) or not isinstance(ch2, discord.VoiceChannel):
        return (
            "⚠️ En löytänyt asetettuja voice-kanavia tai ne eivät ole voice-kanavia.\n"
            "Tarkista TEAM1_VOICE_CHANNEL_ID ja TEAM2_VOICE_CHANNEL_ID."
        )

    async def move_team(team_uids: List[int], channel: discord.VoiceChannel):
        moved = 0
        for uid in team_uids:
            if uid in fake_users:
                continue
            member = guild.get_member(uid)
            if not member:
                continue
            if member.voice and member.voice.channel:
                try:
                    await member.move_to(channel)
                    moved += 1
                except (discord.Forbidden, discord.HTTPException):
                    pass
        return moved

    moved1 = await move_team(team1_ids, ch1)
    moved2 = await move_team(team2_ids, ch2)

    return (
        "Pelaajat, jotka olivat jo voicessa, siirrettiin oikeille kanaville :3"
    )

async def voice_move_countdown(
    interaction: discord.Interaction,
    team1_ids: List[int],
    team2_ids: List[int],
    fake_users: Set[int],
    msg: Optional[discord.Message],
):
    guild = interaction.guild
    if not guild:
        if msg is not None:
            try:
                await msg.edit(content="⚠️ En voinut siirtää pelaajia voiceen (guild puuttuu).")
            except discord.HTTPException:
                pass
        return

    if msg is None:
        if interaction.channel:
            try:
                msg = await interaction.channel.send(
                    "🎧 Pelaajat siirretään voice-kanaville **5s** kuluttua…"
                )
            except discord.HTTPException:
                return
        else:
            return

    seconds = 5
    try:
        for remaining in range(seconds, 0, -1):
            try:
                await msg.edit(
                    content=f"Pelaajat siirretään voice-kanaville **{remaining}s** kuluttua…"
                )
            except discord.HTTPException:
                return
            await asyncio.sleep(1)

        result_text = await move_players_to_fixed_voice_channels(
            guild,
            fake_users=fake_users,
            team1_ids=team1_ids,
            team2_ids=team2_ids,
        )

        try:
            await msg.edit(content=result_text)
        except discord.HTTPException:
            pass

    except asyncio.CancelledError:
        try:
            await msg.edit(content="⚠️ Voice-siirto peruttiin.")
        except Exception:
            pass
        return

async def move_players_to_lobby(guild: discord.Guild, player_ids: List[int]) -> int:
    """Siirtää annetut pelaajat aulakanavalle, jos he ovat jo voice-kanavassa."""
    lobby = guild.get_channel(VOICE_LOBBY_CHANNEL_ID)
    if not isinstance(lobby, discord.VoiceChannel):
        return -1  # tarkoittaa että kanavaa ei löytynyt / väärä tyyppi

    moved = 0
    for uid in player_ids:
        member = guild.get_member(uid)
        if not member:
            continue
        if member.voice and member.voice.channel:
            try:
                await member.move_to(lobby)
                moved += 1
            except (discord.Forbidden, discord.HTTPException):
                pass
    return moved


async def lobby_move_countdown(
    interaction: discord.Interaction,
    all_players: List[int],
    msg: Optional[discord.Message],
):
    """Laskee 15s ja siirtää sen jälkeen kaikki pelin pelaajat aulaan."""
    guild = interaction.guild
    if not guild:
        if msg is not None:
            try:
                await msg.edit(content="⚠️ En voinut siirtää pelaajia aulaan (guild puuttuu).")
            except discord.HTTPException:
                pass
        return

    # Jos viestiä ei ole (esim. prefix-komento + jotain meni pieleen), luodaan uusi
    if msg is None:
        if interaction.channel:
            try:
                msg = await interaction.channel.send(
                    "Pelaajat siirretään aulaan **15s** kuluttua…"
                )
            except discord.HTTPException:
                return
        else:
            return

    seconds = 15
    try:
        for remaining in range(seconds, 0, -1):
            try:
                await msg.edit(
                    content=f"Pelaajat siirretään aulaan **{remaining}s** kuluttua…"
                )
            except discord.HTTPException:
                return
            await asyncio.sleep(1)

        moved = await move_players_to_lobby(guild, all_players)

        if moved == -1:
            text = "⚠️ Aulakanavaa ei löytynyt tai se ei ole voice-kanava."
        else:
            text = f"Siirto valmis! **{moved} pelaajaa** siirrettiin aulaan."

        try:
            await msg.edit(content=text)
        except discord.HTTPException:
            pass

    except asyncio.CancelledError:
        try:
            await msg.edit(content="⚠️ Aulaan siirto peruttiin.")
        except Exception:
            pass
        return


async def backup_db(keep: int = 10):
    src = bot.db.path
    os.makedirs("backups", exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = os.path.join("backups", f"draftbot-{ts}.sqlite3")
    await asyncio.to_thread(shutil.copy2, src, dst)

    files = sorted(
        [f for f in os.listdir("backups") if f.endswith(".sqlite3")]
    )
    for f in files[:-keep]:
        try:
            os.remove(os.path.join("backups", f))
        except Exception:
            pass

class SourceRCON:
    def __init__(self, host: str, port: int, password: str, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._req_id = 0

    def __enter__(self):
        self._socket = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._socket.settimeout(self.timeout)
        if not self.authenticate():
            raise ConnectionError("RCON auth epäonnistui.")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
        self._socket = None

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _send_packet(self, req_id: int, req_type: int, payload: str) -> None:
        if not self._socket:
            raise ConnectionError("RCON socket puuttuu.")
        data = payload.encode("utf-8") + b"\x00\x00"
        packet = struct.pack("<ii", req_id, req_type) + data
        size = struct.pack("<i", len(packet))
        self._socket.sendall(size + packet)

    def _read_packet(self) -> Tuple[int, int, str]:
        if not self._socket:
            raise ConnectionError("RCON socket puuttuu.")
        raw_size = self._socket.recv(4)
        if len(raw_size) < 4:
            raise ConnectionError("RCON vastaus katkesi.")
        (size,) = struct.unpack("<i", raw_size)
        payload = b""
        while len(payload) < size:
            chunk = self._socket.recv(size - len(payload))
            if not chunk:
                break
            payload += chunk
        if len(payload) < 8:
            raise ConnectionError("RCON vastaus liian lyhyt.")
        req_id, req_type = struct.unpack("<ii", payload[:8])
        body = payload[8:-2].decode("utf-8", errors="ignore")
        return req_id, req_type, body

    def authenticate(self) -> bool:
        req_id = self._next_id()
        self._send_packet(req_id, 3, self.password)
        try:
            for _ in range(2):
                resp_id, _resp_type, _body = self._read_packet()
                if resp_id == req_id:
                    return True
                if resp_id == -1:
                    return False
        except Exception:
            return False
        return False

    def command(self, cmd: str) -> str:
        req_id = self._next_id()
        self._send_packet(req_id, 2, cmd)
        resp_id, _resp_type, body = self._read_packet()
        if resp_id == -1:
            raise ConnectionError("RCON komento epäonnistui.")
        return body

def _build_match_config(
    guild_id: int,
    game_id: int,
    selected_map: str,
    team1_players: Dict[str, str],
    team2_players: Dict[str, str],
    team1_side: str,
    team2_side: str,
) -> Tuple[str, dict]:
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"match_{guild_id}_{game_id}_{timestamp}.json"
    config_format = (CS2_MATCH_CONFIG_FORMAT or "matchzy").strip().lower()
    if config_format == "legacy" and (
        "matchzy_loadmatch" in CS2_MATCH_PLUGIN_START_CMD.lower()
        or "matchzy_loadmatch_url" in CS2_MATCH_PLUGIN_START_CMD.lower()
    ):
        config_format = "matchzy"
    if config_format == "legacy":
        team1_ids = list(team1_players.keys())
        team2_ids = list(team2_players.keys())
        config = {
            "map": selected_map,
            "ruleset": "competitive",
            "team1_side": team1_side,
            "team2_side": team2_side,
            "team1": team1_ids,
            "team2": team2_ids,
        }
    else:
        map_side = "team1_ct" if team1_side.upper() == "CT" else "team1_t"
        config = {
            "matchid": str(game_id),
            "num_maps": 1,
            "maplist": [selected_map],
            "map_sides": [map_side],
            "skip_veto": True,
            "team1": {
                "name": "Team 1",
                "players": team1_players,
            },
            "team2": {
                "name": "Team 2",
                "players": team2_players,
            },
        }
    extra = _load_match_config_extras()
    if extra:
        config.update(extra)
    return filename, config

def _load_match_config_extras() -> dict:
    if not CS2_MATCH_CONFIG_EXTRA_JSON:
        return {}
    try:
        payload = json.loads(CS2_MATCH_CONFIG_EXTRA_JSON)
    except json.JSONDecodeError:
        print("CS2_MATCH_CONFIG_EXTRA_JSON ei ole kelvollista JSONia.")
        return {}
    if not isinstance(payload, dict):
        print("CS2_MATCH_CONFIG_EXTRA_JSON pitää olla JSON-objekti.")
        return {}
    return payload

def _match_start_cmds() -> List[str]:
    return [CS2_MATCH_PLUGIN_START_CMD]

def _write_match_config(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    if CS2_MATCH_CONFIG_TARGET_DIR:
        os.makedirs(CS2_MATCH_CONFIG_TARGET_DIR, exist_ok=True)
        target_path = os.path.join(CS2_MATCH_CONFIG_TARGET_DIR, os.path.basename(path))
        shutil.copy2(path, target_path)

def _rcon_start_match(rcon: "SourceRCON", config_filename: str) -> None:
    last_response = ""
    for cmd in _match_start_cmds():
        rcon_arg = _resolve_matchzy_rcon_arg(cmd, config_filename)
        print(f"Lähetetään MatchZy-käynnistys: {cmd} {rcon_arg}")
        response = rcon.command(f"{cmd} {rcon_arg}")
        last_response = response or ""
        if "unknown command" in last_response.lower():
            continue
        if "unknown" in last_response.lower() and "command" in last_response.lower():
            continue
        return
    raise ConnectionError(
        "MatchZy-käynnistys epäonnistui. Tarkista CS2_MATCH_PLUGIN_START_CMD."
        f" Viimeisin vastaus: {last_response}"
    )

def _resolve_matchzy_rcon_path(config_filename: str) -> str:
    if CS2_MATCH_CONFIG_RCON_DIR:
        return os.path.join(CS2_MATCH_CONFIG_RCON_DIR, config_filename).replace("\\", "/")
    if CS2_MATCH_CONFIG_TARGET_DIR:
        normalized = CS2_MATCH_CONFIG_TARGET_DIR.replace("\\", "/")
        lower = normalized.lower()
        marker = "/csgo/"
        idx = lower.find(marker)
        if idx != -1:
            rel = normalized[idx + len(marker):].strip("/")
            if rel:
                return f"{rel}/{config_filename}"
    return config_filename

def _resolve_matchzy_rcon_arg(cmd: str, config_filename: str) -> str:
    lowered = cmd.lower()
    if "matchzy_loadmatch_url" in lowered:
        if not CS2_MATCH_CONFIG_URL_BASE.strip():
            raise ConnectionError(
                "CS2_MATCH_CONFIG_URL_BASE puuttuu matchzy_loadmatch_url-käynnistyksessä."
            )
        base = CS2_MATCH_CONFIG_URL_BASE.strip().replace("\\", "/")
        if base.lower().startswith("http") and not (
            base.lower().startswith("http://") or base.lower().startswith("https://")
        ):
            raise ConnectionError(
                "CS2_MATCH_CONFIG_URL_BASE pitää alkaa http:// tai https:// -osoitteella."
            )
        if "{filename}" in base:
            url = base.replace("{filename}", config_filename)
        else:
            if not base.endswith("/"):
                base = f"{base}/"
            url = f"{base}{config_filename}"
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ConnectionError(
                "CS2_MATCH_CONFIG_URL_BASE pitää olla kelvollinen http(s)-URL."
            )
        return url
    return _resolve_matchzy_rcon_path(config_filename)

def _extract_match_id(payload: dict) -> Optional[str]:
    for key in ("matchid", "match_id", "matchId", "id"):
        if key in payload:
            return str(payload[key])
    match = payload.get("match")
    if isinstance(match, dict):
        for key in ("matchid", "match_id", "matchId", "id"):
            if key in match:
                return str(match[key])
    return None

def _extract_score_pair(payload: dict) -> Optional[Tuple[int, int]]:
    if "team1_score" in payload and "team2_score" in payload:
        try:
            return int(payload["team1_score"]), int(payload["team2_score"])
        except (TypeError, ValueError):
            return None
    match = payload.get("match")
    if isinstance(match, dict):
        return _extract_score_pair(match)
    return None

def _extract_winner(payload: dict) -> Optional[int]:
    if "winner" in payload:
        value = payload["winner"]
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in {"team1", "team_1", "1"}:
                return 1
            if lowered in {"team2", "team_2", "2"}:
                return 2
            if lowered in {"draw", "tie"}:
                return 0
        try:
            winner = int(value)
        except (TypeError, ValueError):
            winner = None
        if winner in (0, 1, 2):
            return winner
    match = payload.get("match")
    if isinstance(match, dict):
        return _extract_winner(match)
    return None

def _parse_winner_value(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"team1", "team_1", "1"}:
            return 1
        if lowered in {"team2", "team_2", "2"}:
            return 2
        if lowered in {"draw", "tie", "0"}:
            return 0
    try:
        winner = int(value)
    except (TypeError, ValueError):
        return None
    if winner in (0, 1, 2):
        return winner
    return None

def _normalize_identifier(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())

def _pick_latest_row(
    rows: List[sqlite3.Row],
    column_map: Dict[str, str],
    started_at_ts: float,
) -> Optional[sqlite3.Row]:
    timestamp_keys = [_normalize_identifier("end_time")]
    timestamp_cols = [column_map[key] for key in timestamp_keys if key in column_map]
    if not timestamp_cols:
        return None
    best_row = None
    best_ts = None

    for row in rows:
        row_ts = None
        for col in timestamp_cols:
            value = row[col]
            if value is None:
                continue
            if isinstance(value, (int, float)):
                row_ts = float(value)
            elif isinstance(value, str):
                try:
                    parsed = datetime.datetime.fromisoformat(value)
                    row_ts = parsed.timestamp()
                except ValueError:
                    continue
            if row_ts is not None:
                break
        if row_ts is None:
            continue
        if row_ts < started_at_ts:
            continue
        if best_ts is None or row_ts > best_ts:
            best_ts = row_ts
            best_row = row

    return best_row

def _row_is_finished(row: sqlite3.Row, column_map: Dict[str, str]) -> bool:
    end_time_keys = [_normalize_identifier("end_time")]
    for key in end_time_keys:
        col = column_map.get(key)
        if not col:
            continue
        value = row[col]
        if value in (None, ""):
            return False
        return True
    return False

def _scan_matchzy_db(game_id: int, started_at_ts: float) -> Optional[Tuple[int, Optional[Tuple[int, int]]]]:
    if not CS2_MATCH_RESULTS_DB:
        return None
    if not os.path.isfile(CS2_MATCH_RESULTS_DB):
        return None
    conn = None
    try:
        conn = sqlite3.connect(CS2_MATCH_RESULTS_DB)
        conn.row_factory = sqlite3.Row
        tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        preferred_table = "matchzy_stats_maps"
        if preferred_table in tables:
            tables = [preferred_table]
        match_id_candidates = {"matchid", "matchid64", "gameid", "id"}
        winner_candidates = {"winner", "winnerteam"}
        score_pairs = [
            ("team1score", "team2score"),
            ("scoreteam1", "scoreteam2"),
        ]

        for table in tables:
            try:
                columns = [
                    row["name"]
                    for row in conn.execute(f"PRAGMA table_info(\"{table}\")")
                ]
            except sqlite3.Error:
                continue
            column_map = {_normalize_identifier(name): name for name in columns}

            match_col = None
            for candidate in match_id_candidates:
                if candidate in column_map:
                    match_col = column_map[candidate]
                    break
            if not match_col:
                continue

            winner_col = None
            for candidate in winner_candidates:
                if candidate in column_map:
                    winner_col = column_map[candidate]
                    break

            score_cols = None
            for left, right in score_pairs:
                if left in column_map and right in column_map:
                    score_cols = (column_map[left], column_map[right])
                    break

            if not winner_col and not score_cols:
                continue

            sql = f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ?"
            try:
                rows = list(conn.execute(sql, (str(game_id),)))
                if not rows:
                    rows = list(conn.execute(sql, (game_id,)))
            except sqlite3.Error:
                continue

            if not rows:
                continue
            row = _pick_latest_row(rows, column_map, started_at_ts)
            if row is None:
                continue
            if not _row_is_finished(row, column_map):
                continue

            winner = _parse_winner_value(row[winner_col]) if winner_col else None
            score_pair = None
            if score_cols:
                try:
                    score_pair = (int(row[score_cols[0]]), int(row[score_cols[1]]))
                except (TypeError, ValueError):
                    score_pair = None

            if winner is None and score_pair:
                if score_pair[0] == score_pair[1]:
                    winner = 0
                else:
                    winner = 1 if score_pair[0] > score_pair[1] else 2

            if winner is None:
                continue
            return winner, score_pair
    except sqlite3.Error:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return None

def _scan_match_results(game_id: int, started_at_ts: float) -> Optional[Tuple[int, Optional[Tuple[int, int]]]]:
    result = _scan_matchzy_db(game_id, started_at_ts)
    if result:
        return result
    return None

def _row_value(row: dict, key: str, default: int = 0) -> int:
    value = row.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _normalize_steamid64(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    text = str(value).strip()
    return text or None

def _compute_population_std(values: List[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((val - mean) ** 2 for val in values) / len(values)
    return math.sqrt(variance)

def _compute_match_ratings(stats: List[dict], rounds: int) -> None:
    if not stats:
        return
    rounds = max(1, rounds)
    for entry in stats:
        entry["kpr"] = entry["kills"] / rounds
        entry["dpr"] = entry["deaths"] / rounds
        entry["adr"] = entry["damage"] / rounds
        entry["apr"] = entry["assists"] / rounds
        mk = (
            entry["enemy2ks"]
            + 2 * entry["enemy3ks"]
            + 3 * entry["enemy4ks"]
            + 4 * entry["enemy5ks"]
        )
        entry["mkpr"] = mk / rounds
        entry["rounds"] = rounds
        entry["kd"] = entry["kills"] / max(1, entry["deaths"])

    def z_score(key: str) -> List[float]:
        values = [entry[key] for entry in stats]
        mean = sum(values) / len(values)
        std = _compute_population_std(values)
        if std == 0:
            return [0.0 for _ in values]
        return [(val - mean) / std for val in values]

    z_kpr = z_score("kpr")
    z_adr = z_score("adr")
    z_mkpr = z_score("mkpr")
    z_apr = z_score("apr")
    z_dpr = z_score("dpr")

    for entry, zk, za, zm, zb, zd in zip(stats, z_kpr, z_adr, z_mkpr, z_apr, z_dpr):
        score = 0.34 * zk + 0.34 * za + 0.16 * zm + 0.10 * zb - 0.14 * zd
        rating = _clip(1.0 + 0.20 * score, 0.0, 2.0)
        entry["rating"] = rating

def _fetch_matchzy_score_pair(game_id: int) -> Optional[Tuple[int, int]]:
    if not CS2_MATCH_RESULTS_DB:
        return None
    if not os.path.isfile(CS2_MATCH_RESULTS_DB):
        return None
    conn = None
    try:
        conn = sqlite3.connect(CS2_MATCH_RESULTS_DB)
        conn.row_factory = sqlite3.Row
        tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if "matchzy_stats_maps" not in tables:
            return None
        table = "matchzy_stats_maps"
        columns = [row["name"] for row in conn.execute(f"PRAGMA table_info(\"{table}\")")]
        column_map = {_normalize_identifier(name): name for name in columns}
        match_col = column_map.get("matchid")
        if not match_col:
            return None
        map_col = column_map.get("mapnumber")
        score_cols = None
        for left, right in (("team1score", "team2score"), ("scoreteam1", "scoreteam2")):
            if left in column_map and right in column_map:
                score_cols = (column_map[left], column_map[right])
                break
        if not score_cols:
            return None
        params = (str(game_id),)
        sql = f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ?"
        rows = list(conn.execute(sql, params))
        if not rows:
            rows = list(conn.execute(sql, (game_id,)))
        if not rows:
            return None
        if map_col:
            zero_rows = [row for row in rows if row[map_col] == 0]
            row = zero_rows[0] if zero_rows else rows[-1]
        else:
            row = rows[-1]
        try:
            return int(row[score_cols[0]]), int(row[score_cols[1]])
        except (TypeError, ValueError):
            return None
    except sqlite3.Error:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return None

def _fetch_matchzy_stats_players(game_id: int) -> List[dict]:
    if not CS2_MATCH_RESULTS_DB:
        return []
    if not os.path.isfile(CS2_MATCH_RESULTS_DB):
        return []
    conn = None
    try:
        conn = sqlite3.connect(CS2_MATCH_RESULTS_DB)
        conn.row_factory = sqlite3.Row
        tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if "matchzy_stats_players" not in tables:
            return []
        table = "matchzy_stats_players"
        columns = [row["name"] for row in conn.execute(f"PRAGMA table_info(\"{table}\")")]
        column_map = {_normalize_identifier(name): name for name in columns}
        match_col = None
        for candidate in ("matchid", "matchid64", "gameid", "id"):
            if candidate in column_map:
                match_col = column_map[candidate]
                break
        if not match_col:
            return []
        map_col = column_map.get("mapnumber")
        params = (str(game_id),)
        rows: List[sqlite3.Row] = []
        if map_col:
            rows = list(
                conn.execute(
                    f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ? AND \"{map_col}\" = 0",
                    params,
                )
            )
            if not rows:
                rows = list(
                    conn.execute(
                        f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ? AND \"{map_col}\" = 0",
                        (game_id,),
                    )
                )
            if not rows:
                cur = conn.execute(
                    f"SELECT MAX(\"{map_col}\") FROM \"{table}\" WHERE \"{match_col}\" = ?",
                    params,
                )
                max_row = cur.fetchone()
                max_map = max_row[0] if max_row else None
                if max_map is not None:
                    rows = list(
                        conn.execute(
                            f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ? AND \"{map_col}\" = ?",
                            params + (max_map,),
                        )
                    )
                    if not rows:
                        rows = list(
                            conn.execute(
                                f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ? AND \"{map_col}\" = ?",
                                (game_id, max_map),
                            )
                        )
        else:
            rows = list(conn.execute(f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ?", params))
            if not rows:
                rows = list(conn.execute(f"SELECT * FROM \"{table}\" WHERE \"{match_col}\" = ?", (game_id,)))
        return [{_normalize_identifier(key): row[key] for key in row.keys()} for row in rows]
    except sqlite3.Error:
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def _build_match_stats(
    game_id: int,
    steamid_to_user: Dict[str, int],
    score_pair: Optional[Tuple[int, int]],
) -> Tuple[List[dict], int]:
    rows = _fetch_matchzy_stats_players(game_id)
    if not rows:
        return [], 0
    if not score_pair:
        score_pair = _fetch_matchzy_score_pair(game_id)
    if score_pair:
        total_rounds = score_pair[0] + score_pair[1]
    else:
        print(f"Match {game_id}: puuttuvat scoret, käytetään fallbackia 30.")
        total_rounds = 30
    if total_rounds <= 0:
        total_rounds = 30
    stats: List[dict] = []
    for row in rows:
        steamid = _normalize_steamid64(row.get("steamid64"))
        if not steamid:
            continue
        user_id = steamid_to_user.get(steamid)
        if not user_id:
            continue
        entry = {
            "user_id": user_id,
            "steamid64": steamid,
            "name": row.get("name") or steamid,
            "team": row.get("team"),
            "kills": _row_value(row, "kills"),
            "deaths": _row_value(row, "deaths"),
            "assists": _row_value(row, "assists"),
            "damage": _row_value(row, "damage"),
            "enemy2ks": _row_value(row, "enemy2ks"),
            "enemy3ks": _row_value(row, "enemy3ks"),
            "enemy4ks": _row_value(row, "enemy4ks"),
            "enemy5ks": _row_value(row, "enemy5ks"),
        }
        stats.append(entry)
    _compute_match_ratings(stats, total_rounds)
    return stats, total_rounds

async def _attach_display_names(
    interaction: discord.Interaction,
    stats: List[dict],
) -> None:
    for entry in stats:
        try:
            entry["display_name"] = await get_display_name(interaction, entry["user_id"])
        except Exception:
            entry["display_name"] = entry.get("name") or str(entry["user_id"])

def _format_match_stats_lines(stats: List[dict]) -> str:
    header = f"{'Player':<18}  {'K':>2}  {'A':>2}  {'D':>2}   {'K/D':>4}  {'ADR':>5}  {'RTG':>4}"
    lines = [header, "-" * len(header)]
    for entry in stats:
        name = entry.get("display_name") or entry.get("name") or str(entry["user_id"])
        name = name[:18]
        lines.append(
            f"{name:<18}  {entry['kills']:>2}  {entry['assists']:>2}  {entry['deaths']:>2}   "
            f"{entry['kd']:>4.2f}  {entry['adr']:>5.1f}  {entry['rating']:>4.2f}"
        )
    return "```\n" + "\n".join(lines) + "\n```"

def _build_match_stats_embed(title: str, stats: List[dict]) -> discord.Embed:
    emb = discord.Embed(title=title, color=EMBED_COLOR_PRIMARY)
    emb.add_field(
        name="K/A/D · K/D · ADR · Rating",
        value=_format_match_stats_lines(stats),
        inline=False,
    )
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    return emb

async def _build_elo_change_report(
    interaction: discord.Interaction,
    game_id: int,
    team1: List[int],
    team2: List[int],
) -> Optional[str]:
    rating_history = await bot.db.get_rating_history_for_game(game_id)
    if not rating_history:
        return None

    async def build_team_lines(team_ids: List[int]) -> List[str]:
        lines: List[str] = []
        for uid in team_ids:
            if uid not in rating_history:
                continue
            name = await get_display_name(interaction, uid)
            _pre, _post, delta = rating_history[uid]
            sign = "+" if delta >= 0 else ""
            lines.append(f"{name:<16} {sign}{delta:.1f}")
        return lines

    team1_lines = await build_team_lines(team1)
    team2_lines = await build_team_lines(team2)

    def block(lines: List[str]) -> str:
        return "```\n" + ("\n".join(lines) if lines else "—") + "\n```"

    return (
        "Elo-muutokset:\n"
        f"Team 1:\n{block(team1_lines)}\n"
        f"Team 2:\n{block(team2_lines)}"
    )

async def watch_match_results(
    interaction: discord.Interaction,
    st: DraftState,
    game_id: int,
    started_at_ts: float,
) -> None:
    try:
        poll_seconds = max(5, CS2_MATCH_RESULTS_POLL_SECONDS)
        deadline = asyncio.get_running_loop().time() + 12 * 60 * 60
        while True:
            await asyncio.sleep(poll_seconds)
            if asyncio.get_running_loop().time() > deadline:
                break
            game = await bot.db.get_game(game_id)
            if not game or game.get("winner") is not None:
                break
            result = await asyncio.to_thread(_scan_match_results, game_id, started_at_ts)
            if not result:
                continue
            winner, score_pair = result
            game = await bot.db.get_game(game_id)
            if not game:
                break
            team1 = game["team1"]
            team2 = game["team2"]
            all_players = team1 + team2
            steam_map = await bot.db.get_steamids(all_players)
            steamid_to_user = {steamid: uid for uid, steamid in steam_map.items()}
            match_stats, total_rounds = await asyncio.to_thread(
                _build_match_stats,
                game_id,
                steamid_to_user,
                score_pair,
            )
            if match_stats:
                await _attach_display_names(interaction, match_stats)
                await bot.db.upsert_match_player_stats(game_id, match_stats)
            else:
                print(f"Match {game_id}: MatchZy-tilastot puuttuvat tai pelaajia ei löytynyt.")
            try:
                if winner == 0:
                    team1, team2 = await bot.db.set_draw(game_id)
                    outcome_text = "Tasapeli"
                else:
                    team1, team2 = await bot.db.set_winner(game_id, winner)
                    outcome_text = f"Voittaja Team {winner}"
            except ValueError:
                break
            score_text = ""
            if score_pair:
                score_text = f" ({score_pair[0]}–{score_pair[1]})"
            if interaction.channel:
                await interaction.channel.send(
                    f"Peli `{game_id}` päättyi. {outcome_text}{score_text}."
                )
                elo_report = await _build_elo_change_report(interaction, game_id, team1, team2)
                if elo_report:
                    await interaction.channel.send(elo_report)
                if match_stats:
                    team1_stats = [s for s in match_stats if s["user_id"] in team1]
                    team2_stats = [s for s in match_stats if s["user_id"] in team2]
                    if team1_stats:
                        await interaction.channel.send(
                            embed=_build_match_stats_embed("Team 1 stats", team1_stats)
                        )
                    if team2_stats:
                        await interaction.channel.send(
                            embed=_build_match_stats_embed("Team 2 stats", team2_stats)
                        )
                countdown_msg = await interaction.channel.send(
                    "Pelaajat siirretään aulaan **15s** kuluttua…"
                )
                shim = SimpleNamespace(guild=interaction.guild, channel=interaction.channel)
                asyncio.create_task(
                    lobby_move_countdown(
                        shim,
                        all_players=all_players,
                        msg=countdown_msg,
                    )
                )
            break
    except asyncio.CancelledError:
        pass
    finally:
        current = asyncio.current_task()
        if st.result_task is current:
            st.result_task = None

async def start_server_orchestration(interaction: discord.Interaction, st: DraftState):
    if not st.team1 or not st.team2 or not st.selected_map:
        await interaction.followup.send("Peliä ei löydy tai karttaa ei ole valittu.")
        return
    if not st.game_id:
        await interaction.followup.send("Pelin ID puuttuu, en voi käynnistää serveriä.")
        return

    all_players = st.team1 + st.team2
    steam_map = await bot.db.get_steamids(all_players)
    missing = [uid for uid in all_players if uid not in steam_map and uid not in st.fake_users]
    if missing:
        missing_mentions = ", ".join(mention(uid) for uid in missing)
        await interaction.followup.send(
            f"Näiltä puuttuu SteamID-linkki: {missing_mentions}\n"
            f"Linkkaa komennolla **/link <steamid64>** tai **!link <steamid64>**."
        )
        return

    if not CS2_RCON_PASSWORD:
        await interaction.followup.send("CS2 RCON salasana puuttuu (CS2_RCON_PASSWORD).")
        return

    def steamid_for(uid: int) -> str:
        if uid in steam_map:
            return steam_map[uid]
        return f"{uid:017d}"

    async def player_name(uid: int) -> str:
        if uid in st.fake_users:
            return f"test-{uid % 1000000}"
        return await get_display_name(interaction, uid)

    team1_players = {steamid_for(uid): await player_name(uid) for uid in st.team1}
    team2_players = {steamid_for(uid): await player_name(uid) for uid in st.team2}
    config_filename, config_data = _build_match_config(
        guild_id=interaction.guild_id or 0,
        game_id=st.game_id,
        selected_map=st.selected_map,
        team1_players=team1_players,
        team2_players=team2_players,
        team1_side=st.team1_side,
        team2_side=st.team2_side,
    )
    try:
        config_path = os.path.join(CS2_MATCH_CONFIG_DIR, config_filename)
        _write_match_config(config_path, config_data)
    except Exception:
        await interaction.followup.send("Match configin kirjoitus epäonnistui.")
        return

    try:
        with SourceRCON(CS2_RCON_HOST, CS2_RCON_PORT, CS2_RCON_PASSWORD) as rcon:
            rcon.command(f"changelevel {st.selected_map}")
            _rcon_start_match(rcon, config_filename)
    except Exception as exc:
        await interaction.followup.send(f"RCON epäonnistui: {exc}")
        return

    if st.result_task and not st.result_task.done():
        st.result_task.cancel()
    if CS2_MATCH_RESULTS_DB and st.game_id:
        started_at_ts = time.time()
        st.result_task = asyncio.create_task(
            watch_match_results(interaction, st, st.game_id, started_at_ts)
        )

    connect_line = (
        f"\nYhdistä: `connect {CS2_SERVER_CONNECT_ADDR}`" if CS2_SERVER_CONNECT_ADDR else ""
    )
    await interaction.followup.send(
        f"Kartta: **{format_map_name(st.selected_map)}**\n"
        f"Puolet: Team 1 **{st.team1_side}** / Team 2 **{st.team2_side}**"
        f"{connect_line}"
    )

async def start_ready_timer(interaction: discord.Interaction, st: DraftState):
    if st.rc_timer_task and not st.rc_timer_task.done():
        st.rc_timer_task.cancel()
    if st.rc_timer_msg:
        try:
            await st.rc_timer_msg.delete()
        except Exception:
            pass
        st.rc_timer_msg = None

    st.rc_deadline_ts = asyncio.get_running_loop().time() + READYCHECK_SECONDS
    st.rc_timer_task = asyncio.create_task(_run_ready_countdown(interaction, st))


async def _run_ready_countdown(interaction: discord.Interaction, st: DraftState):
    try:
        loop = asyncio.get_running_loop()

        def remaining() -> int:
            now = loop.time()
            return int(max(0, (st.rc_deadline_ts or now) - now))

        while st.readycheck_active:
            rem = remaining()
            if rem <= 15:
                break
            await asyncio.sleep(max(0.5, rem - 15))

        if not st.readycheck_active:
            return

        for _ in range(1000):
            rem = remaining()

            text = f"⏳ Readycheck: **{rem}s** aikaa jäljellä… Kirjoita **!r** tai klikkaa nappia!"
            if st.rc_timer_msg is None:
                try:
                    st.rc_timer_msg = await interaction.followup.send(text, ephemeral=False)
                except Exception:
                    if interaction.channel:
                        st.rc_timer_msg = await interaction.channel.send(text)
            else:
                try:
                    await st.rc_timer_msg.edit(content=text)
                except Exception:
                    if interaction.channel:
                        st.rc_timer_msg = await interaction.channel.send(text)

            if rem <= 0 or not st.readycheck_active:
                break
            await asyncio.sleep(1)

        if st.rc_timer_msg:
            try:
                await st.rc_timer_msg.delete()
            except Exception:
                pass
            st.rc_timer_msg = None

    except asyncio.CancelledError:
        if st.rc_timer_msg:
            try:
                await st.rc_timer_msg.delete()
            except Exception:
                pass
        st.rc_timer_msg = None
        return

class ReadyCheckButton(discord.ui.View):
    def __init__(self, bot_instance: 'DraftBot', guild_id: int):
        super().__init__(timeout=READYCHECK_SECONDS)
        self.bot = bot_instance
        self.guild_id = guild_id

    @discord.ui.button(label="PAIKALLA! :3", style=discord.ButtonStyle.green, emoji="✅")
    async def ready_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        st = self.bot.get_state(self.guild_id)

        if not st.readycheck_active:
            return await interaction.response.send_message("Readycheck ei ole enää käynnissä.", ephemeral=True)

        if interaction.user.id not in st.queue:
            return await interaction.response.send_message("Et ole jonossa.", ephemeral=True)

        if interaction.user.id in st.ready_users:
            return await interaction.response.send_message("Olet jo merkinnyt itsesi valmiiksi!", ephemeral=True)

        st.ready_users.add(interaction.user.id)
        left = QUEUE_SIZE - len(st.ready_users)

        if left > 0:
            await interaction.response.send_message(f"✅ Merkitty valmiiksi! Odotetaan vielä {left} pelaajaa…", ephemeral=True)
        else:
            await interaction.response.defer()
            st.readycheck_active = False
            if st.ready_task and not st.ready_task.done():
                st.ready_task.cancel()

            if st.rc_timer_task and not st.rc_timer_task.done():
                st.rc_timer_task.cancel()
            st.rc_timer_task = None
            if st.rc_timer_msg:
                try:
                    await st.rc_timer_msg.delete()
                except Exception:
                    pass
            st.rc_timer_msg = None

            await start_draft(interaction)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

async def ready_timeout_run(interaction: discord.Interaction, st: DraftState):
    try:
        await asyncio.sleep(READYCHECK_SECONDS)

        if not st.readycheck_active:
            return

        if st.rc_timer_task and not st.rc_timer_task.done():
            st.rc_timer_task.cancel()
        st.rc_timer_task = None
        if st.rc_timer_msg:
            try:
                await st.rc_timer_msg.delete()
            except Exception:
                pass
        st.rc_timer_msg = None

        st.readycheck_active = False

        puuttuvat = [u for u in st.queue if u not in st.ready_users]

        if puuttuvat:
            nimet = [await get_display_name(interaction, u) for u in puuttuvat]
            await interaction.followup.send(
                "Readycheck päättyi aikarajaan.\n"
                "Seuraavat eivät vahvistaneet ja poistettiin jonosta: " + ", ".join(nimet)
            )
            st.queue = [u for u in st.queue if u in st.ready_users]
            st.queue_joined_at = {
                uid: joined_at
                for uid, joined_at in st.queue_joined_at.items()
                if uid in st.queue
            }
        else:
            await interaction.followup.send("⏰ Readycheck päättyi.")

        st.ready_users.clear()


    except asyncio.CancelledError:
        if st.rc_timer_task and not st.rc_timer_task.done():
            st.rc_timer_task.cancel()
        st.rc_timer_task = None
        if st.rc_timer_msg:
            try:
                await st.rc_timer_msg.delete()
            except Exception:
                pass
        st.rc_timer_msg = None
        return

class InteractionShim:
    def __init__(self, ctx: commands.Context):
        self._ctx = ctx
        self.guild = ctx.guild
        self.guild_id = ctx.guild.id if ctx.guild else None
        self.user = ctx.author
        self.channel = ctx.channel

        class _Resp:
            async def send_message(_, content=None, *, embed=None, ephemeral=False, view=None):
                return await ctx.reply(content or "", embed=embed, view=view)

            async def defer(_, thinking=False):
                pass

        class _Follow:
            async def send(_, content=None, *, embed=None, ephemeral=False, view=None):
                return await ctx.send(content or "", embed=embed, view=view)

        self.response = _Resp()
        self.followup = _Follow()

# -----------------------------
# Komennot :3
# -----------------------------
@bot.tree.command(name="add", description="Liity jonoon")
async def add_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)
    uid = interaction.user.id
    if st.draft_active or st.readycheck_active:
        return await interaction.response.send_message("Draft käynnissä tai readycheck päällä – ei uusia liittymisiä.", ephemeral=True)
    if uid in st.queue:
        return await interaction.response.send_message("Olet jo jonossa.", ephemeral=True)
    steamid64 = await bot.db.get_steamid(uid)
    if not steamid64:
        return await interaction.response.send_message(
            "Et ole linkannut SteamID:tä. Lisää se komennolla **/link <steamid64>** tai **!link <steamid64>**.",
            ephemeral=True,
        )
    st.queue.append(uid)
    st.queue_joined_at[uid] = datetime.datetime.now(datetime.timezone.utc)
    await interaction.response.send_message(f"Lisätty jonoon. Pelaajia jonossa: {len(st.queue)}/{QUEUE_SIZE}")

    if len(st.queue) >= QUEUE_SIZE and not st.readycheck_active:
        st.readycheck_active = True
        st.ready_users = set()
        st.ready_users.add(uid)
        mentions = " ".join(mention(u) for u in st.queue)
        view = ReadyCheckButton(bot, interaction.guild_id)
        await interaction.followup.send(
            f"**Jonossa 10 pelaajaa!** Readycheck alkaa nyt ({READYCHECK_SECONDS}s).\n"
            f"{mentions}\n"
            f"Klikkaa nappia tai kirjoita **!r** ollaksesi mukana seuraavassa pelissä!",
            view=view
        )
        await start_ready_timer(interaction, st)
        st.ready_task = asyncio.create_task(ready_timeout_run(interaction, st))

@bot.tree.command(name="rm", description="Poistu jonosta")
async def rm_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)
    uid = interaction.user.id
    if uid in st.queue and not st.readycheck_active and not st.draft_active:
        st.queue.remove(uid)
        st.queue_joined_at.pop(uid, None)
        return await interaction.response.send_message("Poistuttu jonosta.")
    return await interaction.response.send_message("Et ole jonossa tai poistuminen ei juuri nyt onnistu.", ephemeral=True)

@bot.tree.command(name="link", description="Linkkaa SteamID64 (ylläpito voi linkata muille)")
@app_commands.describe(steamid64="SteamID64 tai Steam-profiililinkki", user="(Ylläpito) Käyttäjä, jolle linkataan")
async def link_cmd(interaction: discord.Interaction, steamid64: str, user: Optional[discord.User] = None):
    target = user or interaction.user
    if user and not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message(
            "Vain ylläpito voi linkata SteamID:n toiselle käyttäjälle.",
            ephemeral=True,
        )
    raw = steamid64.strip()
    resolved = extract_steamid64(raw)
    if not resolved and "steamcommunity.com/id/" in raw:
        resolved = resolve_vanity_steamid64(raw)
    if not resolved:
        return await interaction.response.send_message("Virheellinen SteamID64 tai profiililinkki.", ephemeral=True)
    if await bot.db.is_steamid_taken(resolved, except_user_id=target.id):
        return await interaction.response.send_message("Tuo SteamID on jo linkattu toiselle.", ephemeral=True)
    await bot.db.upsert_steam_link(target.id, resolved)
    if target.id == interaction.user.id:
        await interaction.response.send_message("SteamID linkattu.")
    else:
        await interaction.response.send_message(f"SteamID linkattu käyttäjälle {target.mention}.")

@bot.tree.command(name="r", description="Merkitse itsesi valmiiksi (readycheck)")
async def r_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)
    if not st.readycheck_active:
        return await interaction.response.send_message("Readycheck ei ole käynnissä.", ephemeral=True)
    if interaction.user.id not in st.queue:
        return await interaction.response.send_message("Et ole jonossa.", ephemeral=True)

    st.ready_users.add(interaction.user.id)
    left = QUEUE_SIZE - len(st.ready_users)

    if left > 0:
        return await interaction.response.send_message(f"Merkitty valmiiksi. Odotetaan vielä {left} pelaajaa…")

    st.readycheck_active = False
    if st.ready_task and not st.ready_task.done():
        st.ready_task.cancel()
    
    if st.rc_timer_task and not st.rc_timer_task.done():
        st.rc_timer_task.cancel()
    st.rc_timer_task = None
    if st.rc_timer_msg:
        try:
            await st.rc_timer_msg.delete()
        except Exception:
            pass
    st.rc_timer_msg = None

    await interaction.response.defer(thinking=False)

    await start_draft(interaction)

async def start_draft(interaction: discord.Interaction):
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)

    if st.draft_active:
        return
    if len(st.queue) < QUEUE_SIZE:
        return await interaction.followup.send("Liian vähän pelaajia draftiin.")

    captain_min_games = 20

    st.readycheck_active = False
    st.ready_users.clear()

    st.last_pick_prefix = None
    st.pick_msg = None
    if st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    st.pick_timer_task = None

    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
    st.timer_msg = None
    await _clear_veto_ui(st)
    st.map_veto_active = False
    st.map_pool = []
    st.banned_maps = []
    st.veto_order = []
    st.veto_index = 0
    st.selected_map = None
    st.side_selection_active = False
    st.side_selection_team = None
    if st.side_selection_msg:
        try:
            await st.side_selection_msg.edit(view=None)
        except Exception:
            pass
    st.side_selection_msg = None
    st.team1_side = "CT"
    st.team2_side = "T"

    pool = st.queue[:QUEUE_SIZE]
    random.shuffle(pool)
    real_pool = [u for u in pool if u not in st.fake_users]
    games_played = await bot.db.get_games_played(real_pool)
    opt_outs = await bot.db.get_captain_opt_outs(real_pool)
    eligible = [
        uid
        for uid in real_pool
        if games_played.get(uid, 0) >= captain_min_games and uid not in opt_outs
    ]
    eligible_without_games = [uid for uid in real_pool if uid not in opt_outs]
    if len(eligible) >= 2:
        captain_pool = eligible
    elif len(eligible_without_games) >= 2:
        captain_pool = eligible_without_games
    else:
        await interaction.followup.send("Liian vähän pelaajia kapteenivalintaan.")
        return

    c1 = random.choice(captain_pool)
    remaining = [uid for uid in captain_pool if uid != c1]
    rating_rows = await bot.db.get_rating_rows(captain_pool)
    c1_rating = rating_rows.get(c1, (INITIAL_RATING, 0))[0]
    closest_diff = min(
        abs(rating_rows.get(uid, (INITIAL_RATING, 0))[0] - c1_rating)
        for uid in remaining
    )
    closest_candidates = [
        uid
        for uid in remaining
        if abs(rating_rows.get(uid, (INITIAL_RATING, 0))[0] - c1_rating) == closest_diff
    ]
    c2 = random.choice(closest_candidates)

    st.captains = (c1, c2)
    st.team1 = [c1]
    st.team2 = [c2]
    await bot.db.bump_captain(c1)
    await bot.db.bump_captain(c2)

    st.pick_pool = [u for u in pool if u not in {c1, c2}]
    st.pick_index = 0
    st.draft_active = True

    st.number_by_uid = {uid: i + 1 for i, uid in enumerate(st.pick_pool)}

    first_turn_team = st.pick_order[st.pick_index] if st.pick_index < len(st.pick_order) else "team1"
    first_turn_label = "Team 1" if first_turn_team == "team1" else "Team 2"

    lines = []
    for u in st.pick_pool:
        disp = await get_display_name(interaction, u) if u not in st.fake_users else f"test-{u % 1000000}"
        lines.append(f"{st.number_by_uid[u]} - {disp}")
    valittavat_block = "```\n" + "\n".join(lines) + "\n```" if lines else "```\n(ei valittavia)\n```"

    cap1_name = await get_display_name(interaction, st.captains[0])
    cap2_name = await get_display_name(interaction, st.captains[1])

    header = (
        f"Readycheck valmis, siirrytään draftiin! Ensimmäisen valinnan tekee: **{first_turn_label}**\n\n"
        f"• Team 1 Kapteeni: {cap1_name}\n"
        f"• Team 2 Kapteeni: {cap2_name}\n"
    )


    await interaction.followup.send(header, ephemeral=False)
    await announce_next_picker(interaction, st)

@bot.tree.command(name="pick", description="Kapteenin valintakomento (esim. /pick 3)")
@app_commands.describe(number="Valittavan pelaajan numero")
async def pick_cmd(interaction: discord.Interaction, number: int):
    st = bot.get_state(interaction.guild_id)
    uid = None
    for k, v in st.number_by_uid.items():
        if v == number:
            uid = k
            break
    if uid is None or uid not in st.pick_pool:
        return await interaction.response.send_message("Virheellinen numero tai pelaaja on jo valittu.", ephemeral=True)

    await handle_pick_selection(interaction, st, uid)

@bot.tree.command(name="setwinner", description="Aseta pelin voittaja numerolla (1=team1, 2=team2; 0=tasan)")
@app_commands.describe(game_id="Pelin ID", winner="Voittanut tiimi (1, 2) tai 0=tasan")
async def setwinner_cmd(interaction: discord.Interaction, game_id: int, winner: int):
    overwrite = (interaction.user.id == 97687348396953600)
    st = bot.get_state(interaction.guild_id)

    try:
        if winner == 0:
            team1, team2 = await bot.db.set_draw(game_id, overwrite=overwrite)
            msg_text = f"Tasapeli tallennettu pelille `{game_id}`."
        elif winner in (1, 2):
            team1, team2 = await bot.db.set_winner(game_id, winner, overwrite=overwrite)
            msg_text = f"Voittaja (team {winner}) tallennettu pelille `{game_id}`."
        else:
            return await interaction.response.send_message("Voittajan tulee olla 0, 1 tai 2.", ephemeral=True)

        elo_report = await _build_elo_change_report(interaction, game_id, team1, team2)
        if elo_report:
            msg_text += f"\n{elo_report}"

        # Lähetetään tulosviesti
        await interaction.response.send_message(msg_text)

        # Käynnistetään 15s countdown aulaan siirtoa varten (voitto TAI tasapeli)
        all_players = team1 + team2

        countdown_msg = None
        try:
            countdown_msg = await interaction.followup.send(
                "Pelaajat siirretään aulaan **15s** kuluttua…"
            )
        except discord.HTTPException:
            pass

        asyncio.create_task(
            lobby_move_countdown(
                interaction,
                all_players=all_players,
                msg=countdown_msg,
            )
        )
        if st.result_task and not st.result_task.done():
            st.result_task.cancel()
        st.result_task = None
    except ValueError as e:
        await interaction.response.send_message(str(e), ephemeral=True)


@setwinner_cmd.autocomplete("game_id")
async def setwinner_game_id_autocomplete(interaction: discord.Interaction, current: str):
    ids = await bot.db.get_recent_game_ids(10)
    if current:
        ids = [gid for gid in ids if str(gid).startswith(current)]
    return [app_commands.Choice(name=f"Peli {gid}", value=gid) for gid in ids]

@bot.tree.command(name="setdraw", description="Merkitse peli tasapeliksi (winner=0)")
async def setdraw_cmd(interaction: discord.Interaction, game_id: int):
    await setwinner_cmd.callback(interaction, game_id, 0)

@bot.tree.command(name="pstats", description="Näytä pelaajan tilastot ja sijoitukset embedinä")
@app_commands.describe(user="Valinnainen: käyttäjä, jonka tilastoja katsotaan")
async def pstats_cmd(interaction: discord.Interaction, user: Optional[discord.User] = None):
    target = user or interaction.user
    data = await bot.db.get_player(target.id)
    if not data:
        return await interaction.response.send_message("Ei tilastoja vielä.", ephemeral=True)

    gp = data["games_played"]
    w  = data["wins"]
    draws = (await bot.db.get_draws_for_users([target.id])).get(target.id, 0)
    wr = ((w + draws * 0.5) / gp * 100.0) if gp > 0 else 0.0

    total_players = await bot.db.count_players()
    r_games   = await bot.db.get_rank("games_played",     target.id)
    r_wins    = await bot.db.get_rank("wins",             target.id)
    r_captain = await bot.db.get_rank("captain_count",    target.id)
    r_first   = await bot.db.get_rank("first_pick_count", target.id)
    r_last    = await bot.db.get_rank("last_pick_count",  target.id)
    await bot.db.ensure_rating(target.id)
    rating_row = await bot.db.get_rating_rows([target.id])
    elo_rating = rating_row.get(target.id, (INITIAL_RATING, 0))[0]
    r_elo = await bot.db.get_elo_rank(target.id)
    pick_turns = await bot.db.get_pick_turns_for_user(target.id, PICK_ORDER)
    avg_pick_round = average_pick_round(pick_turns)

    bot_name = bot.user.name if bot.user else "GatherBot"
    emb = build_stats_embed(
        bot_name=bot_name,
        display_name=target.display_name,
        games=gp, wins=w, winrate=wr,
        captain=data["captain_count"],
        first_picked=data["first_pick_count"],
        last_picked=data["last_pick_count"],
        r_games=r_games, r_wins=r_wins, r_captain=r_captain, r_first=r_first, r_last=r_last,
        total_players=total_players,
        elo_rating=elo_rating,
        r_elo=r_elo,
        avg_pick_round=avg_pick_round,
    )
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="pickstats", description="Näytä millä vuoroilla pelaaja on valittu")
@app_commands.describe(user="Valinnainen: käyttäjä, jonka pickstats katsotaan")
async def pickstats_cmd(interaction: discord.Interaction, user: Optional[discord.User] = None):
    target = user or interaction.user
    pick_turns = await bot.db.get_pick_turns_for_user(target.id, PICK_ORDER)
    avg_pick_round = average_pick_round(pick_turns)
    counts = Counter(pick_turns)
    lines = [f"Vuoro {turn}: {counts[turn]}" for turn in sorted(counts)]
    avg_text = f"{avg_pick_round:.2f}" if avg_pick_round is not None else "—"
    name = await get_display_name(interaction, target.id)

    embed = discord.Embed(
        title=f"Pickstats: {name}",
        color=EMBED_COLOR_PRIMARY,
    )
    embed.add_field(
        name="Valinnat vuoroittain",
        value="\n".join(lines) if lines else "Ei valintoja vielä.",
        inline=False,
    )
    embed.add_field(
        name="Valinnan keskiarvo",
        value=f"Vuoro **{avg_text}**",
        inline=False,
    )
    embed.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="winrate", description="Näytä winrate toista pelaajaa vastaan")
@app_commands.describe(opponent="Pelaaja, jota vastaan", user="Valinnainen: käyttäjä jonka winratea katsotaan")
async def winrate_cmd(
    interaction: discord.Interaction,
    opponent: Optional[discord.User] = None,
    user: Optional[discord.User] = None
):
    target = user or interaction.user
    if opponent is None:
        return await send_head_to_head_summary(interaction, target)
    if target.id == opponent.id:
        return await interaction.response.send_message("Et voi katsoa winratea itseäsi vastaan.", ephemeral=True)

    await send_head_to_head(interaction, target, opponent)

async def send_head_to_head(
    interaction: discord.Interaction,
    target: discord.User,
    opponent: discord.User
) -> None:
    stats = await bot.db.get_head_to_head(target.id, opponent.id)
    if stats["games"] == 0:
        await interaction.response.send_message(
            "Näiden pelaajien välillä ei ole vielä ratkaistuja pelejä.",
            ephemeral=True
        )
        return

    games = stats["games"]
    wr = ((stats["wins"] + stats["draws"] * 0.5) / games * 100.0) if games > 0 else 0.0
    target_name = await get_display_name(interaction, target.id)
    opponent_name = await get_display_name(interaction, opponent.id)

    embed = discord.Embed(
        title=f"Winrate: {target_name} vs {opponent_name}",
        color=EMBED_COLOR_PRIMARY
    )
    embed.add_field(
        name="Pelit",
        value=f"{stats['games']} (W {stats['wins']} / L {stats['losses']} / D {stats['draws']})",
        inline=False
    )
    embed.add_field(
        name="Winrate",
        value=f"{wr:.1f}%",
        inline=False
    )
    embed.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=embed)

async def send_head_to_head_summary(
    interaction: discord.Interaction,
    target: discord.User
) -> None:
    stats_map = await bot.db.get_head_to_head_summary(target.id)
    if not stats_map:
        await interaction.response.send_message(
            "Ei vielä ratkaistuja pelejä tätä pelaajaa vastaan.",
            ephemeral=True,
        )
        return

    rows = []
    for opponent_id, stats in stats_map.items():
        games = stats["games"]
        if games < 5:
            continue
        wr = ((stats["wins"] + stats["draws"] * 0.5) / games * 100.0) if games > 0 else 0.0
        rows.append((opponent_id, stats["wins"], stats["losses"], stats["draws"], games, wr))

    if not rows:
        await interaction.response.send_message(
            "Ei vielä ratkaistuja pelejä tätä pelaajaa vastaan.",
            ephemeral=True,
        )
        return

    best = sorted(rows, key=lambda r: (-r[5], -r[4], r[0]))[:5]
    worst = sorted(rows, key=lambda r: (r[5], -r[4], r[0]))[:5]

    async def build_lines(items: List[Tuple[int, int, int, int, int, float]]) -> str:
        lines = []
        for i, (opponent_id, wins, losses, draws, games, wr) in enumerate(items, start=1):
            name = await get_display_name(interaction, opponent_id)
            wr_suffix = " WR" if i == 1 else ""
            lines.append(
                f"{i}. {name} — {wr:.1f}%{wr_suffix} (W {wins} / L {losses} / D {draws}, {games} peliä)"
            )
        return "\n".join(lines) if lines else "—"

    target_name = await get_display_name(interaction, target.id)
    embed = discord.Embed(
        title=f"Winrate-yhteenveto: {target_name}",
        color=EMBED_COLOR_PRIMARY,
    )
    embed.add_field(
        name="Parhaat winratet (Top 5)",
        value=await build_lines(best),
        inline=False,
    )
    embed.add_field(
        name="Huonoimmat winratet (Top 5)",
        value=await build_lines(worst),
        inline=False,
    )
    embed.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="dstatus", description="Näyttää jonon, valmiit ja draftin tilan")
async def ds_cmd(interaction: discord.Interaction):
    st = bot.get_state(interaction.guild_id)
    if not st.queue and not st.draft_active:
        await interaction.response.send_message("Jono on tyhjä.", ephemeral=True)
        return

    embed = discord.Embed(title="Draftin tila", color=discord.Color.blurple())

    if st.readycheck_active:
        not_ready = [u for u in st.queue if u not in st.ready_users]
        if not_ready:
            names = [await get_display_name(interaction, u) for u in not_ready]
            embed.add_field(
                name=f"Pelaajat, jotka eivät ole vielä valmiina ({len(not_ready)})",
                value="\n".join(names),
                inline=False,
            )
        else:
            embed.add_field(
                name="Kaikki pelaajat ovat valmiina!",
                value="Draft alkaa pian...",
                inline=False,
            )
        qlines = await format_queue_lines(interaction, st.queue, st.queue_joined_at)
        embed.add_field(
            name=f"Jonossa ({len(qlines)})",
            value="\n".join(qlines) if qlines else "—",
            inline=False,
        )

    elif st.draft_active:
        t1_names = [await get_display_name(interaction, u) for u in st.team1]
        t2_names = [await get_display_name(interaction, u) for u in st.team2]
        embed.add_field(
            name=f"Tiimi 1 ({len(t1_names)})",
            value="\n".join(t1_names) if t1_names else "—",
            inline=True,
        )
        embed.add_field(
            name=f"Tiimi 2 ({len(t2_names)})",
            value="\n".join(t2_names) if t2_names else "—",
            inline=True,
        )

    else:
        qnames = await format_queue_lines(interaction, st.queue, st.queue_joined_at)
        embed.add_field(
            name=f"Jonossa ({len(qnames)})",
            value="\n".join(qnames),
            inline=False,
        )

    embed.set_footer(text="CSDraft by Alex")
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name="top10", description="Eniten pelejä pelanneet (Top 10)")
async def top10_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("games_played", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    draw_map = await bot.db.get_draws_for_users([uid for uid, _, _, _ in rows])
    lines = []
    for i, (uid, _, gp, wins) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        draws = draw_map.get(uid, 0)
        wr = ((wins + draws * 0.5) / gp * 100.0) if gp > 0 else 0.0
        lines.append(f"{i}. {name} / {gp}")

    emb = discord.Embed(title="Eniten pelejä pelanneet (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="elo", description="Näytä pelaajan Elo-luku")
@app_commands.describe(user="Valinnainen: käyttäjä, jonka Elo näytetään")
async def elo_cmd(interaction: discord.Interaction, user: Optional[discord.User] = None):
    target = user or interaction.user
    await bot.db.ensure_rating(target.id)
    rows = await bot.db.get_rating_rows([target.id])
    rating, elo_games = rows.get(target.id, (INITIAL_RATING, 0))

    name = await get_display_name(interaction, target.id)
    embed = discord.Embed(
        title=f"Elo: {name}",
        color=EMBED_COLOR_PRIMARY,
    )
    embed.add_field(name="Rating", value=str(int(round(rating))), inline=True)
    embed.add_field(name="Pelit", value=str(int(elo_games)), inline=True)
    if elo_games < 10:
        embed.add_field(name="Status", value="Provisional (min 10 peliä)", inline=False)
    embed.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="topelo", description="Top 10 Elo-ranking")
async def topelo_cmd(interaction: discord.Interaction):
    rows = await bot.db.get_top_ratings(50)
    rows = [row for row in rows if row[2] >= 10]
    if not rows:
        return await interaction.response.send_message(
            "Ei Elo-dataa vielä (min 10 peliä).",
            ephemeral=True,
        )

    lines = []
    for i, (uid, rating, games) in enumerate(rows[:10], start=1):
        name = await get_display_name(interaction, uid)
        lines.append(f"{i}. {name} — {int(round(rating))} ({games} peliä)")

    embed = discord.Embed(
        title="Top 10 Elo",
        description="\n".join(lines),
        color=EMBED_COLOR_PRIMARY,
    )
    embed.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="winners", description="Näytä eniten pelejä voittaneet pelaajat (Top 10)")
async def winners_cmd(interaction: discord.Interaction):
    """Näyttää Top 10 pelaajat voittojen ja voittoprosentin mukaan (tasatilanteet ratkaistaan WR:llä)."""
    async with aiosqlite.connect(bot.db.path) as db:
        cur = await db.execute("SELECT user_id, wins, games_played FROM players")
        players = await cur.fetchall()

    if not players:
        await interaction.response.send_message("Tietokannassa ei ole vielä pelaajia.", ephemeral=True)
        return

    draw_map = await bot.db.get_draws_for_users([uid for uid, _, _ in players])
    rows = []
    for uid, wins, games in players:
        name = await get_display_name(interaction, uid)
        draws = draw_map.get(uid, 0)
        wr = ((wins + draws * 0.5) / games * 100.0) if games > 0 else 0.0
        rows.append((name, wins, games, wr))

    rows.sort(key=lambda r: (-r[1], -r[3], r[0].lower()))

    top = rows[:10]
    lines = []
    for i, (name, wins, games, wr) in enumerate(top, start=1):
        wr_suffix = " WR" if i == 1 else ""
        lines.append(f"{i}. {name} / {wins} ({wr:.1f}%{wr_suffix})")

    embed = discord.Embed(
        title="Eniten pelejä voittaneet (Top 10)",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    embed.set_footer(text="CSDraft by Alex")

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="losers", description="Näytä eniten pelejä hävinneet pelaajat (Top 10)")
async def losers_cmd(interaction: discord.Interaction):
    async with aiosqlite.connect(bot.db.path) as db:
        cur = await db.execute("SELECT user_id, wins, games_played FROM players")
        players = await cur.fetchall()

    if not players:
        await interaction.response.send_message("Tietokannassa ei ole vielä pelaajia.", ephemeral=True)
        return

    draw_map = await bot.db.get_draws_for_users([uid for uid, _, _ in players])
    rows = []
    for uid, wins, games in players:
        draws = draw_map.get(uid, 0)
        losses = max(0, games - wins - draws)
        wr = ((wins + draws * 0.5) / games * 100.0) if games > 0 else 0.0
        name = await get_display_name(interaction, uid)
        rows.append((name, losses, wr))

    rows.sort(key=lambda r: (-r[1], r[2], r[0].lower()))

    top = rows[:10]
    lines = []
    for i, (name, losses, wr) in enumerate(top, start=1):
        wr_suffix = " WR" if i == 1 else ""
        lines.append(f"{i}. {name} / {losses} ({wr:.1f}%{wr_suffix})")

    embed = discord.Embed(
        title="Eniten pelejä hävinneet (Top 10)",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    embed.set_footer(text="CSDraft by Alex")

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="maps", description="Näytä karttojen peluumäärät")
async def maps_cmd(interaction: discord.Interaction):
    counts = await bot.db.get_map_counts()
    all_maps = sorted(
        set(MAP_POOL).union(counts.keys()),
        key=lambda name: (-counts.get(name, 0), format_map_name(name)),
    )
    lines = [f"{format_map_name(map_name)}: {counts.get(map_name, 0)}" for map_name in all_maps]
    embed = discord.Embed(
        title="Karttojen peluumäärät",
        color=EMBED_COLOR_PRIMARY,
        description="\n".join(lines) if lines else "Ei dataa vielä.",
    )
    embed.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="captains", description="Eniten kapteenina toimineet (Top 10)")
async def captains_cmd(interaction: discord.Interaction):
    async with aiosqlite.connect(bot.db.path) as db:
        cur = await db.execute(
            "SELECT user_id, captain_count, captain_wins FROM players ORDER BY captain_count DESC, user_id ASC LIMIT 10"
        )
        rows = await cur.fetchall()
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    lines = []
    for i, (uid, count, wins) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        winrate = (wins / count * 100.0) if count > 0 else 0.0
        wr_suffix = " WR" if i == 1 else ""
        lines.append(f"{i}. {name} / {count} ({winrate:.1f}%{wr_suffix})")

    emb = discord.Embed(title="Eniten kapteenina toimineet (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="thinkids", description="Eniten valittu ensimmäisenä (Top 10)")
async def thinkids_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("first_pick_count", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    winrate_map = await bot.db.get_pick_winrates(
        [uid for uid, _, _, _ in rows],
        pick_index=1,
    )
    lines = []
    for i, (uid, count, _, _) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        stats = winrate_map.get(uid, {"games": 0, "wins": 0, "draws": 0})
        games = stats["games"]
        wr = ((stats["wins"] + stats["draws"] * 0.5) / games * 100.0) if games > 0 else 0.0
        wr_suffix = " WR" if i == 1 else ""
        lines.append(f"{i}. {name} / {count} ({wr:.1f}%{wr_suffix})")

    emb = discord.Embed(title="Eniten valittu ensimmäisenä (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="fatkids", description="Eniten valittu viimeisenä (Top 10)")
async def fatkids_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("last_pick_count", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    winrate_map = await bot.db.get_pick_winrates(
        [uid for uid, _, _, _ in rows],
        pick_index=-1,
    )
    lines = []
    for i, (uid, count, _, _) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        stats = winrate_map.get(uid, {"games": 0, "wins": 0, "draws": 0})
        games = stats["games"]
        wr = ((stats["wins"] + stats["draws"] * 0.5) / games * 100.0) if games > 0 else 0.0
        wr_suffix = " WR" if i == 1 else ""
        lines.append(f"{i}. {name} / {count} ({wr:.1f}%{wr_suffix})")

    emb = discord.Embed(title="Eniten valittu viimeisenä (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="reset", description="Tyhjennä jono (admin)" )
async def reset_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("Vain ylläpito voi nollata jonon.", ephemeral=True)
    st = bot.get_state(interaction.guild_id)
    st.queue.clear(); st.queue_joined_at.clear(); st.ready_users.clear(); st.readycheck_active = False
    if st.ready_task and not st.ready_task.done():
        st.ready_task.cancel()
    if st.result_task and not st.result_task.done():
        st.result_task.cancel()
    st.result_task = None
    st.draft_active = False
    st.captains = None
    st.last_pick_prefix = None
    st.team1.clear(); st.team2.clear(); st.pick_pool.clear(); st.pick_index = 0
    await _clear_veto_ui(st)
    st.map_veto_active = False
    st.map_pool = []
    st.banned_maps = []
    st.veto_order = []
    st.veto_index = 0
    st.selected_map = None
    st.side_selection_active = False
    st.side_selection_team = None
    if st.side_selection_msg:
        try:
            await st.side_selection_msg.edit(view=None)
        except Exception:
            pass
    st.side_selection_msg = None
    st.team1_side = "CT"
    st.team2_side = "T"
    await interaction.response.send_message("Jono ja draft-tila nollattu.")

@bot.tree.command(name="nocaptain", description="Estä bottia valitsemasta sinua kapteeniksi")
async def nocaptain_cmd(interaction: discord.Interaction):
    await bot.db.set_captain_opt_out(interaction.user.id, True)
    await interaction.response.send_message("Sinua ei enää valita kapteeniksi.", ephemeral=True)

@bot.tree.command(name="allowcaptain", description="Salli botin valita sinut kapteeniksi")
async def allowcaptain_cmd(interaction: discord.Interaction):
    await bot.db.set_captain_opt_out(interaction.user.id, False)
    await interaction.response.send_message("Sinut voidaan jälleen valita kapteeniksi.", ephemeral=True)

@bot.tree.command(name="recalcelo", description="Laske Elo-pisteet uudelleen kaikista peleistä (admin)")
async def recalcelo_cmd(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("Vain ylläpito voi käyttää tätä komentoa.", ephemeral=True)
    processed = await bot.db.recalc_all_ratings_from_history()
    await interaction.response.send_message(f"Elo-laskenta valmis. Käsiteltiin {processed} peliä.")
    
@bot.tree.command(name="filltest", description="Täyttää jonon testipelaajilla (vain kehityskäyttöön).")
async def filltest_cmd(interaction: discord.Interaction):
    if interaction.user.id != 97687348396953600:
        await interaction.response.send_message("Sinulla ei ole oikeutta käyttää tätä komentoa.", ephemeral=True)
        return
        
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)
    uid = interaction.user.id

    if uid not in st.queue:
        st.queue.append(uid)
        st.queue_joined_at[uid] = datetime.datetime.now(datetime.timezone.utc)

    needed = max(0, QUEUE_SIZE - len(st.queue))

    base = 900000
    added = 0
    i = 1
    while added < needed:
        fid = base + i
        i += 1
        if fid in st.queue:
            continue
        st.queue.append(fid)
        st.queue_joined_at[fid] = datetime.datetime.now(datetime.timezone.utc)
        st.fake_users.add(fid)
        added += 1

    await interaction.response.send_message(
        f"Lisätty {added} testipelaajaa. Jonossa nyt {len(st.queue)}/{QUEUE_SIZE}.",
        ephemeral=True
    )

    if len(st.queue) >= QUEUE_SIZE and not st.readycheck_active and not st.draft_active:
        st.readycheck_active = True
        st.ready_users = set()
        st.ready_users.update(st.fake_users)
        real_mentions = " ".join(mention(u) for u in st.queue if u not in st.fake_users)
        view = ReadyCheckButton(bot, interaction.guild_id)
        await interaction.followup.send(
            "**Testimoodi:** Feikkipelaajat merkitty valmiiksi.\n"
            f"{real_mentions}\n"
            f"Oikeat pelaajat: klikkaa nappia tai kirjoita **!r** {READYCHECK_SECONDS} sekunnin sisällä.",
            view=view
        )
        await start_ready_timer(interaction, st)
        st.ready_task = asyncio.create_task(ready_timeout_run(interaction, st))

@bot.command(name="link")
async def link_bang(ctx: commands.Context, *args: str):
    if not args:
        return await ctx.reply("Käyttö: `!link <steamid64>` tai `!link @user <steamid64>`")

    target = ctx.author
    raw = None
    if len(args) == 1:
        raw = args[0]
    else:
        try:
            target = await commands.MemberConverter().convert(ctx, args[0])
        except commands.BadArgument:
            target = ctx.author
        if target != ctx.author and not ctx.author.guild_permissions.manage_guild:
            return await ctx.reply("Vain ylläpito voi linkata SteamID:n toiselle käyttäjälle.")
        raw = args[1] if target != ctx.author else args[0]

    if raw is None:
        return await ctx.reply("Käyttö: `!link <steamid64>` tai `!link @user <steamid64>`")

    resolved = extract_steamid64(raw.strip())
    if not resolved and "steamcommunity.com/id/" in raw:
        resolved = resolve_vanity_steamid64(raw)
    if not resolved:
        return await ctx.reply("Virheellinen SteamID64 tai profiililinkki.")
    if await bot.db.is_steamid_taken(resolved, except_user_id=target.id):
        return await ctx.reply("Tuo SteamID on jo linkattu toiselle.")
    await bot.db.upsert_steam_link(target.id, resolved)
    if target.id == ctx.author.id:
        await ctx.reply("SteamID linkattu.")
    else:
        await ctx.reply(f"SteamID linkattu käyttäjälle {target.mention}.")

@bot.command(name="unlink")
async def unlink_bang(ctx: commands.Context):
    existing = await bot.db.get_steamid(ctx.author.id)
    if not existing:
        return await ctx.reply("Sinulla ei ole linkkiä.")
    await bot.db.delete_steam_link(ctx.author.id)
    await ctx.reply("SteamID-linkki poistettu.")

@bot.command(name="mysteam")
async def mysteam_bang(ctx: commands.Context):
    steamid64 = await bot.db.get_steamid(ctx.author.id)
    if not steamid64:
        return await ctx.reply("Et ole linkannut SteamID:tä.")
    try:
        await ctx.author.send(f"SteamID64: {steamid64}")
        await ctx.reply("Lähetin SteamID:n DM:ään.")
    except Exception:
        await ctx.reply(f"SteamID64: {mask_steamid64(steamid64)}")

@bot.command(name="startserver")
async def startserver_bang(ctx: commands.Context):
    if not ctx.author.guild_permissions.manage_guild:
        return await ctx.reply("Vain ylläpito voi käynnistää serverin.")
    interaction = InteractionShim(ctx)
    st = bot.get_state(ctx.guild.id if ctx.guild else 0)
    await start_server_orchestration(interaction, st)

@bot.command(name="add", aliases=["dad", "bad", "ad", "dab", "sad", "mad", "dda", "aada", "addme", "da", "meadd", "lisää", "lisaa", "adam", "peliä", "pelejä", "peli", "ass", "addd", "addista", "addistä", "adidas", "lisäyskomento", "lisäää", "lissää", "moti100", "join", "play", "pelataan", "pistämutjonoon", "gaming", "messiin", "liity", "mukaan", "lisäys", "nike", "puma", "josonpakko", "askel", "bugatti", "sievi", "kuoma", "jalas", "taasmenään", "hyllymbvör","eioomuutakaantekemistä","newbalance","onkopakko","asics","ejendals","roka","erect","suutujo","cs"])
async def add_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await add_cmd.callback(interaction)

@bot.command(name="rm", aliases=["remove", "nah", "nvm", "moti0", "liikaaslurreja", "pois", "poistu", "gg", "vitut","kännissä","känisä","fuck","imeparsaa"])
async def rm_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await rm_cmd.callback(interaction)

@bot.command(name="r", aliases=["ready"])
async def r_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await r_cmd.callback(interaction)

@bot.command(name="reset")
async def reset_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await reset_cmd.callback(interaction)

@bot.command(name="dstatus")
async def dstatus_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await ds_cmd.callback(interaction)

@bot.command(name="pstats")
async def pstats_bang(ctx: commands.Context, user: Optional[discord.Member] = None):
    interaction = InteractionShim(ctx)
    await pstats_cmd.callback(interaction, user or ctx.author)

@bot.command(name="pickstats")
async def pickstats_bang(ctx: commands.Context, user: Optional[discord.Member] = None):
    interaction = InteractionShim(ctx)
    await pickstats_cmd.callback(interaction, user or ctx.author)

@bot.command(name="winrate", aliases=["wr"])
async def winrate_bang(ctx: commands.Context, *, opponent: Optional[str] = None):
    if opponent is None:
        interaction = InteractionShim(ctx)
        return await send_head_to_head_summary(interaction, ctx.author)
    opponent_user = await resolve_user_from_text(ctx.guild, opponent)
    if not opponent_user:
        return await ctx.reply("En löytänyt vastustajaa annetulla nimellä tai ID:llä.")
    if opponent_user.id == ctx.author.id:
        return await ctx.reply("Et voi katsoa winratea itseäsi vastaan.")
    interaction = InteractionShim(ctx)
    await send_head_to_head(interaction, ctx.author, opponent_user)


@bot.command(name="pick", aliases=["p"])
async def pick_bang(ctx: commands.Context, number: int):
    interaction = InteractionShim(ctx)
    await pick_cmd.callback(interaction, number)

@bot.command(name="setwinner")
async def setwinner_bang(ctx: commands.Context, game_id: int, winner: int):
    interaction = InteractionShim(ctx)
    await setwinner_cmd.callback(interaction, game_id, winner)

@bot.command(name="top10")
async def top10_bang(ctx: commands.Context):
    await top10_cmd.callback(InteractionShim(ctx))

@bot.command(name="winners")
async def winners_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await winners_cmd.callback(interaction)

@bot.command(name="losers")
async def losers_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await losers_cmd.callback(interaction)

@bot.command(name="maps")
async def maps_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await maps_cmd.callback(interaction)

@bot.command(name="captains")
async def captains_bang(ctx: commands.Context):
    await captains_cmd.callback(InteractionShim(ctx))

@bot.command(name="thinkids")
async def thinkids_bang(ctx: commands.Context):
    await thinkids_cmd.callback(InteractionShim(ctx))

@bot.command(name="fatkids")
async def fatkids_bang(ctx: commands.Context):
    await fatkids_cmd.callback(InteractionShim(ctx))
    
@bot.command(name="setdraw", aliases=["sd"])
async def setdraw_bang(ctx: commands.Context, game_id: int = None):
    if game_id is None:
        return await ctx.reply("Käyttö: `!setdraw <peli_id>`")
    await setwinner_cmd.callback(InteractionShim(ctx), game_id, 0)

@bot.command(name="nocaptain")
async def nocaptain_bang(ctx: commands.Context):
    await nocaptain_cmd.callback(InteractionShim(ctx))

@bot.command(name="allowcaptain")
async def allowcaptain_bang(ctx: commands.Context):
    await allowcaptain_cmd.callback(InteractionShim(ctx))

@bot.command(name="elo")
async def elo_bang(ctx: commands.Context, user: Optional[discord.Member] = None):
    interaction = InteractionShim(ctx)
    await elo_cmd.callback(interaction, user or ctx.author)

@bot.command(name="csstats")
async def csstats_bang(ctx: commands.Context, *, target: Optional[str] = None):
    user = ctx.author
    if target:
        resolved = await resolve_user_from_text(ctx.guild, target)
        if not resolved:
            return await ctx.reply("En löytänyt käyttäjää annetulla nimellä tai ID:llä.")
        user = resolved

    steamid64 = await bot.db.get_steamid(user.id)
    if not steamid64:
        return await ctx.reply("Käyttäjällä ei ole SteamID-linkkiä.")

    stats = await bot.db.get_match_player_stats_for_user(user.id)
    if not stats:
        return await ctx.reply("Tilastoja ei löytynyt vielä yhdestäkään ottelusta.")

    games = len(stats)
    total_kills = sum(entry["kills"] for entry in stats)
    total_deaths = sum(entry["deaths"] for entry in stats)
    avg_kills = total_kills / games
    avg_kd = total_kills / max(1, total_deaths)
    avg_adr = sum(entry["adr"] for entry in stats) / games
    avg_rating = sum(entry["rating"] for entry in stats) / games

    max_kills = max(entry["kills"] for entry in stats)
    max_kd = max(entry["kd"] for entry in stats)
    max_adr = max(entry["adr"] for entry in stats)
    max_rating = max(entry["rating"] for entry in stats)

    emb = discord.Embed(
        title=f"CS Stats — {user.display_name}",
        color=EMBED_COLOR_PRIMARY,
    )
    emb.add_field(name="Pelit", value=str(games), inline=False)
    emb.add_field(
        name="Keskiarvot",
        value=(
            f"**Kills:** {round(avg_kills)}\n"
            f"**K/D:** {avg_kd:.2f}\n"
            f"**ADR:** {round(avg_adr)}\n"
            f"**Rating:** {avg_rating:.2f}"
        ),
        inline=False,
    )
    emb.add_field(
        name="Ennätykset",
        value=(
            f"**Kills:** {max_kills}\n"
            f"**K/D:** {max_kd:.2f}\n"
            f"**ADR:** {round(max_adr)}\n"
            f"**Rating:** {max_rating:.2f}"
        ),
        inline=False,
    )
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await ctx.reply(embed=emb)

@bot.command(name="topelo")
async def topelo_bang(ctx: commands.Context):
    await topelo_cmd.callback(InteractionShim(ctx))

@bot.command(name="recalcelo")
async def recalcelo_bang(ctx: commands.Context):
    await recalcelo_cmd.callback(InteractionShim(ctx))

# -----------------------------
# Käynnistys :3
# -----------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

if __name__ == "____main__":
    pass

if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit("Aseta DISCORD_TOKEN ympäristömuuttujaan.")
    bot.run(token)
