import os
import asyncio
import random
import json
import shutil
import datetime
import typing
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
AUTO_VOICE_CHANNELS = True
TEAM1_VOICE_CHANNEL_ID = 85232997602709504
TEAM2_VOICE_CHANNEL_ID = 131440492314492928
VOICE_LOBBY_CHANNEL_ID = 85232997602709504

# List of admin user IDs
ADMIN_IDS = [97687348396953600 # Alex
            ,231712366981677056]  # Snowblind

# ---- UI: värit ja footer ----
EMBED_COLOR_PRIMARY = 0x29377e
EMBED_FOOTER_TEXT   = "CSDraft by Alex"

def build_stats_embed(
    bot_name: str,
    display_name: str,
    games: int, wins: int, winrate: float,
    captain: int, first_picked: int, last_picked: int,
    r_games: int, r_wins: int, r_captain: int, r_first: int, r_last: int,
    total_players: int
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

    emb.set_footer(text="CSDraft by Alex")
    return emb

# -----------------------------
@dataclass
class DraftState:
    queue: List[int] = field(default_factory=list)
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
    pick_order: List[str] = field(default_factory=lambda: [
        "team1", "team2", "team1", "team2", "team1", "team2", "team2"
    ])
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
    pick_view: Optional[discord.ui.View] = None


# -----------------------------
# Database tsydeemi :3
# -----------------------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS players (
  user_id INTEGER PRIMARY KEY,
  games_played INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
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
"""

class DB:
    def __init__(self, path: str = "draftbot.sqlite3") -> None:
        self.path = path
        self._lock = asyncio.Lock()

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA_SQL)
            await db.commit()

    async def ensure_player(self, user_id: int):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO players (user_id) VALUES (?)",
                    (user_id,),
                )
                await db.commit()

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

    async def record_game(self, guild_id: int, team1: List[int], team2: List[int]) -> int:
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

                if previous_winner is None:
                    await db.execute("UPDATE games SET winner=? WHERE id=?", (winner_team, game_id))
                    winners = team1 if winner_team == 1 else team2
                    for uid in winners:
                        await db.execute("UPDATE players SET wins = wins + 1 WHERE user_id = ?", (uid,))
                    await db.commit()
                    return team1, team2

                if not overwrite:
                    raise ValueError("Tälle pelille on jo asetettu voittaja.")
                    
                if previous_winner == winner_team:
                    await db.commit()
                    return team1, team2
                    
                prev_winners = team1 if previous_winner == 1 else team2
                new_winners  = team1 if winner_team == 1 else team2

                for uid in prev_winners:
                    await db.execute("UPDATE players SET wins = wins - 1 WHERE user_id = ?", (uid,))
                for uid in new_winners:
                    await db.execute("UPDATE players SET wins = wins + 1 WHERE user_id = ?", (uid,))

                await db.execute("UPDATE games SET winner=? WHERE id=?", (winner_team, game_id))
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

                if previous_winner is None:
                    await db.execute("UPDATE games SET winner=0 WHERE id=?", (game_id,))
                    await db.commit()
                    return team1, team2

                if previous_winner in (1, 2):
                    if not overwrite:
                        raise ValueError("Tälle pelille on jo asetettu voittaja.")
                    prev_winners = team1 if previous_winner == 1 else team2
                    for uid in prev_winners:
                        await db.execute("UPDATE players SET wins = wins - 1 WHERE user_id = ?", (uid,))
                    await db.execute("UPDATE games SET winner=0 WHERE id=?", (game_id,))
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

def calculate_winrate(wins: int, draws: int, games: int) -> float:
    if games <= 0:
        return 0.0
    return ((wins + draws * 0.5) / games) * 100.0
    
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
        f"Valitse pelaaja komennolla **!pick numero** tai käytä nappeja alla\n\n"
        f"{remaining_block}\n"
    )

    # Stop old view if it exists
    if st.pick_view:
        st.pick_view.stop()

    # Create new picker view with buttons
    picker_view = await create_picker_view(bot, interaction.guild_id, st, interaction)
    st.pick_view = picker_view

    if st.pick_msg:
        try:
            st.pick_msg = await st.pick_msg.edit(content=content, view=picker_view)
        except Exception:
            st.pick_msg = await interaction.followup.send(content, view=picker_view, ephemeral=False)
    else:
        st.pick_msg = await interaction.followup.send(content, view=picker_view, ephemeral=False)

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
    lines = []
    for u in st.pick_pool:
        name = await get_display_name(interaction, u) if u not in st.fake_users else f"test-{u % 1000000}"
        num = st.number_by_uid.get(u, "?")
        lines.append(f"{num} - {name}")
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



async def _apply_pick(interaction: discord.Interaction, st: DraftState, uid: int, is_autopick: bool = False):
    if st.pick_index >= len(st.pick_order):
        return

    current_team = st.pick_order[st.pick_index]
    target_team = st.team1 if current_team == "team1" else st.team2

    # Disable button view since pick is being made
    if st.pick_view:
        st.pick_view.stop()
        for item in st.pick_view.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        st.pick_view = None

    if uid in st.pick_pool:
        target_team.append(uid)
        st.pick_pool.remove(uid)

    if st.pick_index == 0:
        await bot.db.bump_first_last(uid, first=True)

    st.pick_index += 1

    team_num = 1 if current_team == "team1" else 2
    picked_name = f"test-{uid % 1000000}" if uid in st.fake_users else await get_display_name(interaction, uid)
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


async def _finish_or_next(interaction: discord.Interaction, st: DraftState):
    if st.pick_index >= len(st.pick_order):
        if len(st.pick_pool) == 1:
            last_uid = st.pick_pool.pop()
            st.team1.append(last_uid)
            await bot.db.bump_first_last(last_uid, last=True)

        game_id = await bot.db.record_game(interaction.guild_id, st.team1, st.team2)
        st.game_id = game_id

        await backup_db()

        names1 = [ (f"test-{u % 1000000}" if u in st.fake_users else await get_display_name(interaction, u)) for u in st.team1 ]
        names2 = [ (f"test-{u % 1000000}" if u in st.fake_users else await get_display_name(interaction, u)) for u in st.team2 ]

        emb = discord.Embed(title="Valitut joukkueet", color=EMBED_COLOR_PRIMARY)
        emb.add_field(name="Team1:", value=("\n".join(names1) if names1 else "-"), inline=True)
        emb.add_field(name="Team2:", value=("\n".join(names2) if names2 else "-"), inline=True)
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
        st.draft_active = False
        st.captains = None
        st.team1.clear(); st.team2.clear(); st.pick_pool.clear()
        st.pick_index = 0
        st.number_by_uid.clear()
        st.last_pick_prefix = None 

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

        # Clean up button view
        if st.pick_view:
            st.pick_view.stop()
            st.pick_view = None

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


async def create_picker_view(
    bot_instance: 'DraftBot',
    guild_id: int,
    st: DraftState,
    interaction: discord.Interaction
) -> 'PickerView':
    """Factory function to create and initialize PickerView asynchronously."""
    team = st.pick_order[st.pick_index]
    expected_captain = st.captains[0] if team == "team1" else st.captains[1]

    view = PickerView(
        bot_instance=bot_instance,
        guild_id=guild_id,
        pick_pool=list(st.pick_pool),
        number_by_uid=dict(st.number_by_uid),
        expected_captain_id=expected_captain,
        fake_users=st.fake_users,
        interaction_for_names=interaction
    )

    await view.initialize_buttons()
    return view


class PickerView(discord.ui.View):
    """Interactive button view for captain player selection."""

    def __init__(
        self,
        bot_instance: 'DraftBot',
        guild_id: int,
        pick_pool: List[int],
        number_by_uid: Dict[int, int],
        expected_captain_id: int,
        fake_users: Set[int],
        interaction_for_names: discord.Interaction
    ):
        super().__init__(timeout=PICK_TIMEOUT_SECONDS)
        self.bot = bot_instance
        self.guild_id = guild_id
        self.expected_captain_id = expected_captain_id
        self.fake_users = fake_users
        self.interaction_for_names = interaction_for_names

        self._pick_pool = pick_pool
        self._number_by_uid = number_by_uid

    async def initialize_buttons(self):
        """Async initialization to fetch player names and create buttons."""
        for uid in self._pick_pool:
            player_number = self._number_by_uid[uid]

            # Get display name
            if uid in self.fake_users:
                display_name = f"test-{uid % 1000000}"
            else:
                display_name = await get_display_name(self.interaction_for_names, uid)

            # Truncate name if too long (button labels have 80 char limit)
            label = f"{player_number} - {display_name[:20]}"

            # Create button - 2 per row for vertical layout
            button = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.primary,
                custom_id=f"pick_{self.guild_id}_{uid}",
                row=(player_number - 1) // 2  # 2 buttons per row
            )

            # Assign callback
            button.callback = self._create_pick_callback(uid)
            self.add_item(button)

        return self

    def _create_pick_callback(self, uid: int):
        """Create button callback for specific player."""
        async def pick_callback(interaction: discord.Interaction):
            st = self.bot.get_state(self.guild_id)

            # Validation 1: Draft active
            if not st.draft_active:
                return await interaction.response.send_message(
                    "Draft ei ole käynnissä.", ephemeral=True
                )

            # Validation 2: Authorization
            if interaction.user.id != self.expected_captain_id:
                return await interaction.response.send_message(
                    "Ei ole sinun vuorosi valita.", ephemeral=True
                )

            # Validation 3: Player available
            if uid not in st.pick_pool:
                return await interaction.response.send_message(
                    "Tämä pelaaja on jo valittu.", ephemeral=True
                )

            # Validation 4: Pick index valid
            if st.pick_index >= len(st.pick_order):
                return await interaction.response.send_message(
                    "Kaikki pelaajat on jo valittu.", ephemeral=True
                )

            # Defer immediately to prevent timeout
            await interaction.response.defer(thinking=False)

            # Stop current view to disable all buttons
            if st.pick_view:
                st.pick_view.stop()
                st.pick_view = None

            # Apply the pick using existing logic
            await _apply_pick(interaction, st, uid, is_autopick=False)

        return pick_callback

    async def on_timeout(self):
        """Disable all buttons when timeout occurs."""
        for item in self.children:
            if isinstance(item, discord.ui.Button):
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
    await interaction.response.send_message(f"Lisätty jonoon. Pelaajia jonossa: {len(st.queue)}/{QUEUE_SIZE}")

    if len(st.queue) >= QUEUE_SIZE and not st.readycheck_active:
        st.readycheck_active = True
        st.ready_users = set()
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

    pool = st.queue[:QUEUE_SIZE]
    random.shuffle(pool)
    real_pool = [u for u in pool if u not in st.fake_users]
    if len(real_pool) >= 2:
        c1, c2 = real_pool[0], real_pool[1]
    elif len(real_pool) == 1:
        # Ensure the one real player is captain 1
        c1 = real_pool[0]
        fake_pool = [u for u in pool if u in st.fake_users]
        c2 = fake_pool[0] if fake_pool else pool[1]
    else:
        # No real players (unlikely), use any two from pool
        c1, c2 = pool[0], pool[1]

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
    if not st.draft_active:
        return await interaction.response.send_message("Draft ei ole käynnissä.", ephemeral=True)
    if st.pick_index >= len(st.pick_order):
        return await interaction.response.send_message("Kaikki pelaajat on jo valittu.", ephemeral=True)

    team = st.pick_order[st.pick_index]
    expected_captain = st.captains[0] if team == "team1" else st.captains[1]
    if interaction.user.id != expected_captain:
        return await interaction.response.send_message("Ei ole sinun vuorosi valita.", ephemeral=True)

    uid = None
    for k, v in st.number_by_uid.items():
        if v == number:
            uid = k
            break
    if uid is None or uid not in st.pick_pool:
        return await interaction.response.send_message("Virheellinen numero tai pelaaja on jo valittu.", ephemeral=True)

    await interaction.response.defer(thinking=False)

    # Disable button view since command pick is being made
    if st.pick_view:
        st.pick_view.stop()
        st.pick_view = None

    target_team = st.team1 if team == "team1" else st.team2
    target_team.append(uid)
    st.pick_pool.remove(uid)

    if st.pick_index == 0:
        await bot.db.bump_first_last(uid, first=True)

    st.pick_index += 1

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

    team_num = 1 if team == "team1" else 2
    name = f"test-{uid % 1000000}" if uid in st.fake_users else await get_display_name(interaction, uid)
    st.last_pick_prefix = f"Pelaaja {name} lisätty tiimiin {team_num}."
    
    await _finish_or_next(interaction, st)

@bot.tree.command(name="setwinner", description="Aseta pelin voittaja numerolla (1=team1, 2=team2; 0=tasan)")
@app_commands.describe(game_id="Pelin ID", winner="Voittanut tiimi (1, 2) tai 0=tasan")
async def setwinner_cmd(interaction: discord.Interaction, game_id: int, winner: int):
    overwrite = (interaction.user.id in ADMIN_IDS)

    try:
        if winner == 0:
            team1, team2 = await bot.db.set_draw(game_id, overwrite=overwrite)
            msg_text = f"Tasapeli tallennettu pelille `{game_id}`."
        elif winner in (1, 2):
            team1, team2 = await bot.db.set_winner(game_id, winner, overwrite=overwrite)
            msg_text = f"Voittaja (team {winner}) tallennettu pelille `{game_id}`."
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
    wr = calculate_winrate(w, draws, gp)

    total_players = await bot.db.count_players()
    r_games   = await bot.db.get_rank("games_played",     target.id)
    r_wins    = await bot.db.get_rank("wins",             target.id)
    r_captain = await bot.db.get_rank("captain_count",    target.id)
    r_first   = await bot.db.get_rank("first_pick_count", target.id)
    r_last    = await bot.db.get_rank("last_pick_count",  target.id)

    bot_name = bot.user.name if bot.user else "GatherBot"
    emb = build_stats_embed(
        bot_name=bot_name,
        display_name=target.display_name,
        games=gp, wins=w, winrate=wr,
        captain=data["captain_count"],
        first_picked=data["first_pick_count"],
        last_picked=data["last_pick_count"],
        r_games=r_games, r_wins=r_wins, r_captain=r_captain, r_first=r_first, r_last=r_last,
        total_players=total_players
    )
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="winrate", description="Näytä winrate toista pelaajaa vastaan")
@app_commands.describe(opponent="Pelaaja, jota vastaan", user="Valinnainen: käyttäjä jonka winratea katsotaan")
async def winrate_cmd(
    interaction: discord.Interaction,
    opponent: discord.User,
    user: Optional[discord.User] = None
):
    target = user or interaction.user
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

    wr = calculate_winrate(stats["wins"], stats["draws"], stats["games"])
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
        qnames = [await get_display_name(interaction, u) for u in st.queue]
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
        wr = calculate_winrate(wins, draw_map.get(uid, 0), gp)
        lines.append(f"{i}. {name} / {gp}")

    emb = discord.Embed(title="Eniten pelejä pelanneet (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

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
        wr = calculate_winrate(wins, draw_map.get(uid, 0), games)
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
    rows = await bot.db.leaderboard("captain_count", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    lines = []
    for i, (uid, count, _, _) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        lines.append(f"{i}. {name} / {count}")

    emb = discord.Embed(title="Eniten kapteenina toimineet (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="thinkids", description="Eniten valittu ensimmäisenä (Top 10)")
async def thinkids_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("first_pick_count", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    lines = []
    for i, (uid, count, _, _) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        lines.append(f"{i}. {name} / {count}")

    emb = discord.Embed(title="Eniten valittu ensimmäisenä (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="fatkids", description="Eniten valittu viimeisenä (Top 10)")
async def fatkids_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("last_pick_count", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    lines = []
    for i, (uid, count, _, _) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        lines.append(f"{i}. {name} / {count}")

    emb = discord.Embed(title="Eniten valittu viimeisenä (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="reset", description="Tyhjennä jono (admin)" )
async def reset_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("Vain ylläpito voi nollata jonon.", ephemeral=True)
    st = bot.get_state(interaction.guild_id)
    st.queue.clear(); st.ready_users.clear(); st.readycheck_active = False
    if st.ready_task and not st.ready_task.done():
        st.ready_task.cancel()
    st.draft_active = False
    st.captains = None
    st.last_pick_prefix = None
    st.team1.clear(); st.team2.clear(); st.pick_pool.clear(); st.pick_index = 0
    if st.pick_view:
        st.pick_view.stop()
    st.pick_view = None
    await interaction.response.send_message("Jono ja draft-tila nollattu.")
    
@bot.tree.command(name="filltest", description="Täyttää jonon testipelaajilla (vain kehityskäyttöön).")
async def filltest_cmd(interaction: discord.Interaction):
    if interaction.user.id not in ADMIN_IDS:
        await interaction.response.send_message("Sinulla ei ole oikeutta käyttää tätä komentoa.", ephemeral=True)
        return
        
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)
    uid = interaction.user.id

    if uid not in st.queue:
        st.queue.append(uid)

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

@bot.command(name="add", aliases=["dad", "bad", "ad", "dab", "sad", "mad", "dda", "aada", "addme", "da", "meadd", "lisää", "lisaa", "adam", "peliä", "pelejä", "peli", "ass", "addd", "addista", "addistä", "adidas", "lisäyskomento", "lisäää", "lissää", "moti100", "join", "play", "pelataan", "pistämutjonoon", "gaming", "messiin", "liity", "mukaan", "lisäys"])
async def add_bang(ctx: commands.Context):
    interaction = InteractionShim(ctx)
    await add_cmd.callback(interaction)

@bot.command(name="rm", aliases=["remove", "nah", "nvm", "moti0", "liikaaslurreja"])
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

@bot.command(name="winrate", aliases=["wr"])
async def winrate_bang(ctx: commands.Context, *, opponent: Optional[str] = None):
    if opponent is None:
        return await ctx.reply("Käyttö: `!winrate <nimi tai ID>`")
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
