import os
import asyncio
import random
import json
import shutil
import datetime
import socket
import struct
import typing
import re
import urllib.request
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
MAP_VETO_TIMEOUT_SECONDS = 45
AUTO_VOICE_CHANNELS = True
TEAM1_VOICE_CHANNEL_ID = 1442861436542910494
TEAM2_VOICE_CHANNEL_ID = 1442861481564831785
VOICE_LOBBY_CHANNEL_ID = 364497233061871628
CS2_RCON_HOST = os.getenv("CS2_RCON_HOST", "127.0.0.1")
CS2_RCON_PORT = int(os.getenv("CS2_RCON_PORT", "27015"))
CS2_RCON_PASSWORD = os.getenv("CS2_RCON_PASSWORD", "")
CS2_MATCH_CONFIG_DIR = os.getenv("CS2_MATCH_CONFIG_DIR", "./match_configs")
CS2_MATCH_PLUGIN_START_CMD = os.getenv("CS2_MATCH_PLUGIN_START_CMD", "css_matchzy_start")
CS2_SERVER_CONNECT_ADDR = os.getenv("CS2_SERVER_CONNECT_ADDR", "")

# ---- UI: värit ja footer ----
EMBED_COLOR_PRIMARY = 0x29377e
EMBED_FOOTER_TEXT   = "CSDraft by Alex"

PICK_ORDER = [
    "team1", "team2", "team1", "team2", "team1", "team2", "team2"
]
MAP_POOL = [
    "de_ancient",
    "de_anubis",
    "de_inferno",
    "de_mirage",
    "de_nuke",
    "de_overpass",
    "de_vertigo",
]

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
                new_rating = rating + delta

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
                        delta,
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
            label=map_name,
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
    remaining_text = ", ".join(remaining) if remaining else "—"
    banned_text = ", ".join(st.banned_maps) if st.banned_maps else "—"
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

    prefix = f"Kartta **{map_name}** bannattu."
    if is_autoban:
        prefix = f"Aikaraja! Kartta **{map_name}** bannattu automaattisesti."
    await announce_next_veto(interaction, st, prefix=prefix)

async def _finish_map_veto(interaction: discord.Interaction, st: DraftState, selected_map: Optional[str]):
    st.map_veto_active = False
    st.selected_map = selected_map
    await _clear_veto_ui(st)

    if not selected_map:
        await interaction.followup.send("Karttaveto epäonnistui: karttaa ei löytynyt.")
        return

    await interaction.followup.send(f"**Kartta valittu:** {selected_map}")
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
                "Pelaajat siirretään voice-kanaville **15s** kuluttua…"
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
                    "🎧 Pelaajat siirretään voice-kanaville **15s** kuluttua…"
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
    team1_ids: List[str],
    team2_ids: List[str],
) -> Tuple[str, dict]:
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"match_{guild_id}_{game_id}_{timestamp}.json"
    config = {
        "map": selected_map,
        "ruleset": "competitive",
        "team1_side": "CT",
        "team2_side": "T",
        "team1": team1_ids,
        "team2": team2_ids,
    }
    return filename, config

async def start_server_orchestration(interaction: discord.Interaction, st: DraftState):
    if not st.team1 or not st.team2 or not st.selected_map:
        await interaction.followup.send("Peliä ei löydy tai karttaa ei ole valittu.")
        return
    if not st.game_id:
        await interaction.followup.send("Pelin ID puuttuu, en voi käynnistää serveriä.")
        return

    all_players = st.team1 + st.team2
    steam_map = await bot.db.get_steamids(all_players)
    missing = [uid for uid in all_players if uid not in steam_map]
    if missing:
        missing_mentions = ", ".join(mention(uid) for uid in missing)
        await interaction.followup.send(
            f"Näiltä puuttuu SteamID-linkki: {missing_mentions}\n"
            f"Linkkaa komennolla **!link <steamid64>**."
        )
        return

    if not CS2_RCON_PASSWORD:
        await interaction.followup.send("CS2 RCON salasana puuttuu (CS2_RCON_PASSWORD).")
        return

    os.makedirs(CS2_MATCH_CONFIG_DIR, exist_ok=True)
    team1_steamids = [steam_map[uid] for uid in st.team1]
    team2_steamids = [steam_map[uid] for uid in st.team2]
    config_filename, config_data = _build_match_config(
        guild_id=interaction.guild_id or 0,
        game_id=st.game_id,
        selected_map=st.selected_map,
        team1_ids=team1_steamids,
        team2_ids=team2_steamids,
    )
    config_path = os.path.join(CS2_MATCH_CONFIG_DIR, config_filename)
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2)
    except Exception:
        await interaction.followup.send("Match configin kirjoitus epäonnistui.")
        return

    try:
        with SourceRCON(CS2_RCON_HOST, CS2_RCON_PORT, CS2_RCON_PASSWORD) as rcon:
            rcon.command(f"changelevel {st.selected_map}")
            rcon.command(f"{CS2_MATCH_PLUGIN_START_CMD} {config_filename}")
    except Exception as exc:
        await interaction.followup.send(f"RCON epäonnistui: {exc}")
        return

    team1_names = " ".join(mention(uid) for uid in st.team1)
    team2_names = " ".join(mention(uid) for uid in st.team2)
    connect_line = f"\nYhdistä: `{CS2_SERVER_CONNECT_ADDR}`" if CS2_SERVER_CONNECT_ADDR else ""
    await interaction.followup.send(
        f"Serveri käynnistetään kartalla **{st.selected_map}**.\n"
        f"**Team 1 (CT):** {team1_names}\n"
        f"**Team 2 (T):** {team2_names}\n"
        f"Match start -komento lähetetty.{connect_line}"
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

    try:
        if winner == 0:
            team1, team2 = await bot.db.set_draw(game_id, overwrite=overwrite)
            msg_text = f"Tasapeli tallennettu pelille `{game_id}`."
        elif winner in (1, 2):
            team1, team2 = await bot.db.set_winner(game_id, winner, overwrite=overwrite)
            msg_text = f"Voittaja (team {winner}) tallennettu pelille `{game_id}`."

            rating_history = await bot.db.get_rating_history_for_game(game_id)
            if rating_history:
                async def build_team_changes(team_ids: List[int]) -> Tuple[str, float]:
                    parts = []
                    team_delta: Optional[float] = None
                    for uid in team_ids:
                        if uid not in rating_history:
                            continue
                        name = await get_display_name(interaction, uid)
                        _pre, post, delta = rating_history[uid]
                        if team_delta is None:
                            team_delta = round(delta, 1)
                        parts.append(f"{name} ({int(round(post))})")
                    return (", ".join(parts) if parts else "—", team_delta or 0.0)

                team1_changes, team1_delta = await build_team_changes(team1)
                team2_changes, team2_delta = await build_team_changes(team2)
                team1_sign = "+" if team1_delta >= 0 else ""
                team2_sign = "+" if team2_delta >= 0 else ""
                msg_text += (
                    "\nElo-muutokset:"
                    f"\nTeam 1 ({team1_sign}{team1_delta:.1f}): {team1_changes}"
                    f"\nTeam 2 ({team2_sign}{team2_delta:.1f}): {team2_changes}"
                )
        else:
            return await interaction.response.send_message("Voittajan tulee olla 0, 1 tai 2.", ephemeral=True)

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
            lines.append(
                f"{i}. {name} — {wr:.1f}% (W {wins} / L {losses} / D {draws}, {games} peliä)"
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
    lines = [
        f"{i}. {name} / {wins} ({wr:.1f}%)"
        for i, (name, wins, games, wr) in enumerate(top, start=1)
    ]

    embed = discord.Embed(
        title="Eniten pelejä voittaneet (Top 10)",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    embed.set_footer(text="CSDraft by Alex")

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
        lines.append(f"{i}. {name} / {count} ({winrate:.1f}%)")

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
        lines.append(f"{i}. {name} / {count} ({wr:.1f}%)")

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
        lines.append(f"{i}. {name} / {count} ({wr:.1f}%)")

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
async def link_bang(ctx: commands.Context, steamid64: str):
    raw = steamid64.strip()
    resolved = extract_steamid64(raw)
    if not resolved and "steamcommunity.com/id/" in raw:
        resolved = resolve_vanity_steamid64(raw)
    if not resolved:
        return await ctx.reply("Virheellinen SteamID64 tai profiililinkki.")
    if await bot.db.is_steamid_taken(resolved, except_user_id=ctx.author.id):
        return await ctx.reply("Tuo SteamID on jo linkattu toiselle.")
    await bot.db.upsert_steam_link(ctx.author.id, resolved)
    await ctx.reply("SteamID linkattu.")

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
