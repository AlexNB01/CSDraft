import os
import asyncio
import random
import json
import shutil
import datetime
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
READYCHECK_SECONDS = 45
GUILD_SCOPED = True
PICK_TIMEOUT_SECONDS = 30


# ---- UI: värit ja footer ----
EMBED_COLOR_PRIMARY = 0x6B4EFF
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
    #fake_users: Set[int] = field(default_factory=set)
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
    
    game_id: Optional[int] = None

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
        """
        Aseta voittaja. Jos overwrite=True ja peliin on jo merkitty voittaja,
        muutetaan tulosta: vanhalta voittajatiimiltä vähennetään 1 voitto / pelaaja
        ja uudelta voittajatiimiltä lisätään 1 voitto / pelaaja.
        Palauttaa (team1, team2) listat.
        """
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
        """Palauta pelin tiedot: team1, team2, winner (tai None jos ei löydy)."""
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
        """
        Palauta pelaajan sijoitus (1=paras).
        - Jos arvo == 0  -> näytetään viimeinen sijoitus (kaikille nollissa oleville sama numero).
        - Muuten         -> dense ranking: 1 + count(>arvo).
        """
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


# -----------------------------
# Bot :3
# -----------------------------
class DraftBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = False
        intents.message_content = False
        super().__init__(command_prefix="!", intents=intents)
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

    # Yhdistä viime valinnan ilmoitus alkuun (jos annettu tai tallessa)
    head = prefix if prefix is not None else (st.last_pick_prefix or "")
    st.last_pick_prefix = None  # tyhjennä käytön jälkeen
    if head and not head.endswith("\n"):
        head += "\n"

    remaining_block = await build_remaining_block(interaction, st)
    content = (
        f"{head}"
        f"Seuraava vuoro: {mention(captain)}\n\n"
        f"{remaining_block}\n"
    )

    if st.pick_msg:
        try:
            st.pick_msg = await st.pick_msg.edit(content=content)
        except Exception:
            st.pick_msg = await interaction.followup.send(content, ephemeral=False)
    else:
        st.pick_msg = await interaction.followup.send(content, ephemeral=False)

    # Käynnistä tai nollaa ajastin ja näytä se heti
    await start_pick_timer(interaction, st)


async def start_pick_timer(interaction: discord.Interaction, st: DraftState):
    """Käynnistä countdown heti näkyvällä timer-viestillä, päivitys 1s välein."""
    # sammuta aiempi countdown
    if st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    # poista vanha timer-viesti
    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
        st.timer_msg = None

    # deadline ja viesti heti näkyviin
    st.pick_deadline_ts = asyncio.get_running_loop().time() + PICK_TIMEOUT_SECONDS
    try:
        st.timer_msg = await interaction.followup.send(f"⏳ **{PICK_TIMEOUT_SECONDS}s** aikaa valita…", ephemeral=False)
    except Exception:
        # jos followup kaatuu (harvoin), lähetä kanavaan
        if interaction.channel:
            st.timer_msg = await interaction.channel.send(f"⏳ **{PICK_TIMEOUT_SECONDS}s** aikaa valita…")

    # käynnistä sekuntikello
    st.pick_timer_task = asyncio.create_task(_run_pick_countdown(interaction, st))


async def build_remaining_block(interaction: discord.Interaction, st: DraftState) -> str:
    lines = []
    for u in st.pick_pool:
        name = await get_display_name(interaction, u) if u not in st.fake_users else f"test-{u % 1000000}"
        num = st.number_by_uid.get(u, "?")
        lines.append(f"{num} - {name}")
    return "```\nValittavissa:\n" + "\n".join(lines) + "\n```" if lines else "```\nValittavissa:\n(ei ketään)\n```"

async def _run_pick_countdown(interaction: discord.Interaction, st: DraftState):
    try:
        loop = asyncio.get_running_loop()

        def remaining() -> int:
            now = loop.time()
            return int(max(0, (st.pick_deadline_ts or now) - now))

        # päivitä sekunnin välein nollaan
        while st.draft_active and st.pick_index < len(st.pick_order):
            rem = remaining()

            # päivitä viesti
            if st.timer_msg:
                try:
                    await st.timer_msg.edit(content=f"⏳ **{rem}s** aikaa valita…")
                except Exception:
                    # jos edit ei onnistu (esim. viesti poistettu), luo uusi
                    if interaction.channel and rem > 0:
                        st.timer_msg = await interaction.channel.send(f"⏳ **{rem}s** aikaa valita…")

            if rem <= 0:
                break
            await asyncio.sleep(1)

        # aika loppui -> autopick ja LOPETA tämä taski heti
        if st.draft_active and st.pick_index < len(st.pick_order) and st.pick_pool:
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

    # lisää joukkueeseen
    if uid in st.pick_pool:
        target_team.append(uid)
        st.pick_pool.remove(uid)

    # tilasto: eka pick
    if st.pick_index == 0:
        await bot.db.bump_first_last(uid, first=True)

    st.pick_index += 1

    # Rakenna seuraavan vuoron prefiksi
    team_num = 1 if current_team == "team1" else 2
    picked_name = f"test-{uid % 1000000}" if uid in st.fake_users else await get_display_name(interaction, uid)
    st.last_pick_prefix = f"Pelaaja {picked_name} lisätty tiimiin {team_num}."

    # Siivoa timer-viesti (molemmissa tapauksissa)
    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
        st.timer_msg = None

    # Manuaalinen pick: pysäytä käynnissä ollut countdown-task
    if not is_autopick and st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    st.pick_timer_task = None

    # Aina uusi 'Seuraava vuoro' -viesti ja timer, jos draft jatkuu
    if st.pick_index < len(st.pick_order):
        st.pick_msg = None  # pakota uuden viestin luonti
        await announce_next_picker(interaction, st)
        return

    # Muuten: draft päättyi → hoidetaan viimeinen pelaaja, tallennus ja embed
    await _finish_or_next(interaction, st)


async def _finish_or_next(interaction: discord.Interaction, st: DraftState):
    # Kaikki pickit tehty?
    if st.pick_index >= len(st.pick_order):
        # Jos yksi vielä jäljellä, se menee Team1:lle
        if len(st.pick_pool) == 1:
            last_uid = st.pick_pool.pop()
            st.team1.append(last_uid)
            await bot.db.bump_first_last(last_uid, last=True)

        # Tallenna peli
        game_id = await bot.db.record_game(interaction.guild_id, st.team1, st.team2)
        st.game_id = game_id

        # DB-varmistus
        await backup_db()

        # Rakenna nimet ilman tägäyksiä
        names1 = [ (f"test-{u % 1000000}" if u in st.fake_users else await get_display_name(interaction, u)) for u in st.team1 ]
        names2 = [ (f"test-{u % 1000000}" if u in st.fake_users else await get_display_name(interaction, u)) for u in st.team2 ]

        emb = discord.Embed(title="Selected teams", color=EMBED_COLOR_PRIMARY)
        emb.add_field(name="Team1:", value=("\n".join(names1) if names1 else "-"), inline=True)
        emb.add_field(name="Team2:", value=("\n".join(names2) if names2 else "-"), inline=True)
        emb.set_footer(text="CSDraft by Alex")

        await interaction.followup.send(
            content=(
                f"**Draft valmis!** Pelin ID: `{game_id}`\n"
                f"Aseta voittaja: `/setwinner {game_id} 1` tai `/setwinner {game_id} 2`."
            ),
            embed=emb
        )

        # Siivoa tila
        drafted = set(st.team1 + st.team2)
        st.queue = [u for u in st.queue if u not in drafted]
        st.draft_active = False
        st.captains = None
        st.team1.clear(); st.team2.clear(); st.pick_pool.clear()
        st.pick_index = 0
        st.number_by_uid.clear()

        # Pysäytä ja poista mahdolliset viestit/taskit
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
        return

    # --- Seuraava vuoro ---
    # Varmista ettei edellisen vuoron timer roiku
    if st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()
    st.pick_timer_task = None
    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
    st.timer_msg = None

    # Luo seuraavan vuoron viesti (mukana mahdollinen st.last_pick_prefix) ja käynnistä uusi timer
    await announce_next_picker(interaction, st)


async def backup_db(keep: int = 10):
    """Kopioi SQLite-tietokanta backups/-kansioon ja karsii vanhat varmistukset."""
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
    """Käynnistä readycheckin countdown. Viesti tulee näkyviin vasta kun aikaa on ≤ 15s."""
    # sammuta vanha RC-timer
    if st.rc_timer_task and not st.rc_timer_task.done():
        st.rc_timer_task.cancel()
    # poista vanha viesti
    if st.rc_timer_msg:
        try:
            await st.rc_timer_msg.delete()
        except Exception:
            pass
        st.rc_timer_msg = None

    st.rc_deadline_ts = asyncio.get_running_loop().time() + READYCHECK_SECONDS
    st.rc_timer_task = asyncio.create_task(_run_ready_countdown(interaction, st))


async def _run_ready_countdown(interaction: discord.Interaction, st: DraftState):
    """Näytä viesti, kun aikaa ≤ 15s; päivitä 1s välein; poista kun päättyy."""
    try:
        loop = asyncio.get_running_loop()

        def remaining() -> int:
            now = loop.time()
            return int(max(0, (st.rc_deadline_ts or now) - now))

        # Odota, kunnes aikaa on ≤ 15s tai readycheck päättyy
        while st.readycheck_active:
            rem = remaining()
            if rem <= 15:
                break
            # nukutaan kerralla rem-15 sekuntia (min 0.5s safetyna)
            await asyncio.sleep(max(0.5, rem - 15))

        # Jos readycheck ehti päättyä, lopeta hiljaa
        if not st.readycheck_active:
            return

        # Luo viesti ja päivitä sitä sekunnin välein
        for _ in range(1000):
            rem = remaining()

            # päivitä/näytä viesti
            text = f"⏳ Readycheck: **{rem}s** aikaa jäljellä… Kirjoita **/r**!"
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
                    # jos edit epäonnistui, yritetään luoda uusi
                    if interaction.channel:
                        st.rc_timer_msg = await interaction.channel.send(text)

            if rem <= 0 or not st.readycheck_active:
                break
            await asyncio.sleep(1)

        # Valmistuminen tai aika loppui -> siivoa viesti
        if st.rc_timer_msg:
            try:
                await st.rc_timer_msg.delete()
            except Exception:
                pass
            st.rc_timer_msg = None

    except asyncio.CancelledError:
        # peruttu siististi -> poista viesti jos on
        if st.rc_timer_msg:
            try:
                await st.rc_timer_msg.delete()
            except Exception:
                pass
        st.rc_timer_msg = None
        return

async def ready_timeout_run(interaction: discord.Interaction, st: DraftState):
    try:
        await asyncio.sleep(READYCHECK_SECONDS)

        if not st.readycheck_active:
            return

        # Siivoa ready-timer (viesti+task)
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
                "Seuraavat eivät vahvistaneet: " + ", ".join(nimet)
            )
        else:
            await interaction.followup.send("⏰ Readycheck päättyi.")
        st.ready_users.clear()

    except asyncio.CancelledError:
        # Siivoa myös peruutuksessa
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
        await interaction.followup.send(
            f"**Jonossa 10 pelaajaa!** Readycheck alkaa nyt ({READYCHECK_SECONDS}s).\n"
            f"{mentions}\n"
            f"Kirjoittakaa **/r** ollaksenne mukana seuraavassa pelissä!."
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

    # Estä kaksoiskäynnistys ja varmistu pelaajamäärästä
    if st.draft_active:
        return
    if len(st.queue) < QUEUE_SIZE:
        return await interaction.followup.send("Liian vähän pelaajia draftiin.")

    # Nollaa readycheck ja aloita draft
    st.readycheck_active = False
    st.ready_users.clear()

    # Arvo kapteenit: suosi oikeita käyttäjiä jos mahdollista
    pool = st.queue[:QUEUE_SIZE]
    random.shuffle(pool)
    real_pool = [u for u in pool if u not in st.fake_users]
    if len(real_pool) >= 2:
        c1, c2 = real_pool[0], real_pool[1]
    else:
        c1, c2 = pool[0], pool[1]

    st.captains = (c1, c2)
    st.team1 = [c1]
    st.team2 = [c2]
    # Päivitä kapteenitilastot tietokantaan
    await bot.db.bump_captain(c1)
    await bot.db.bump_captain(c2)


    # Valittavat pelaajat = kaikki muut kuin kapteenit
    st.pick_pool = [u for u in pool if u not in {c1, c2}] # kapteenit ovat pool[0] ja pool[1]
    st.pick_index = 0
    st.draft_active = True

    # Numeroi valittavat (1–8)
    st.number_by_uid = {uid: i + 1 for i, uid in enumerate(st.pick_pool)}

    # Kumpi aloittaa
    first_turn_team = st.pick_order[st.pick_index] if st.pick_index < len(st.pick_order) else "team1"
    first_turn_label = "Team 1" if first_turn_team == "team1" else "Team 2"

    # Rakennetaan yksi aloitusviesti (SUOMEKSI): kapteenit + ohje + valittavat
    lines = []
    for u in st.pick_pool:
        disp = await get_display_name(interaction, u) if u not in st.fake_users else f"test-{u % 1000000}"
        lines.append(f"{st.number_by_uid[u]} - {disp}")
    valittavat_block = "```\n" + "\n".join(lines) + "\n```" if lines else "```\n(ei valittavia)\n```"

    header = (
        f"Readycheck valmis, siirrytään draftiin! Ensimmäisen valinnan tekee: **{first_turn_label}**\n\n"
        f"• Team 1 Kapteeni: {mention(st.captains[0])}\n"
        f"• Team 2 Kapteeni: {mention(st.captains[1])}\n"
        f"Valitse pelaaja komennolla **/pick NUMERO**\n\n"
        f"Valittavissa:\n"
    )

    await interaction.followup.send(header + valittavat_block, ephemeral=False)
    await start_pick_timer(interaction, st)

@bot.tree.command(name="pick", description="Kapteenin valintakomento (esim. /pick 3)")
@app_commands.describe(number="Valittavan pelaajan numero")
async def pick_cmd(interaction: discord.Interaction, number: int):
    st = bot.get_state(interaction.guild_id)
    if not st.draft_active:
        return await interaction.response.send_message("Draft ei ole käynnissä.", ephemeral=True)
    if st.pick_index >= len(st.pick_order):
        return await interaction.response.send_message("Kaikki pelaajat on jo valittu.", ephemeral=True)

    # Vain oikea kapteeni voi valita
    team = st.pick_order[st.pick_index]
    expected_captain = st.captains[0] if team == "team1" else st.captains[1]
    if interaction.user.id != expected_captain:
        return await interaction.response.send_message("Ei ole sinun vuorosi valita.", ephemeral=True)

    # Tarkistetaan valinnan numero
    uid = None
    for k, v in st.number_by_uid.items():
        if v == number:
            uid = k
            break
    if uid is None or uid not in st.pick_pool:
        return await interaction.response.send_message("Virheellinen numero tai pelaaja on jo valittu.", ephemeral=True)

    await interaction.response.defer(thinking=False)

    # Lisää pelaaja oikeaan joukkueeseen
    target_team = st.team1 if team == "team1" else st.team2
    target_team.append(uid)
    st.pick_pool.remove(uid)

    # Tilastot
    if st.pick_index == 0:
        await bot.db.bump_first_last(uid, first=True)

    st.pick_index += 1

    # Pysäytä countdown (manuaalinen pick ei ole autopick)
    if st.pick_timer_task and not st.pick_timer_task.done():
        st.pick_timer_task.cancel()

    # Poista mahdollinen timer-viesti
    if st.timer_msg:
        try:
            await st.timer_msg.delete()
        except Exception:
            pass
        st.timer_msg = None

    # Seuraavalla vuorolla tehdään uusi 'Seuraava vuoro' -viesti
    st.pick_msg = None

    # Ilmoita valinnasta ja rakenna seuraavan vuoron prefiksi
    team_num = 1 if team == "team1" else 2
    name = f"test-{uid % 1000000}" if uid in st.fake_users else await get_display_name(interaction, uid)
    st.last_pick_prefix = f"Pelaaja {name} lisätty tiimiin {team_num}."
    
    # Seuraava vuoro tai draftin päätös
    await _finish_or_next(interaction, st)


@bot.tree.command(name="setwinner", description="Aseta pelin voittaja numerolla (1=team1, 2=team2)")
@app_commands.describe(game_id="Pelin ID", winner="Voittanut tiimi (1 tai 2)")
async def setwinner_cmd(interaction: discord.Interaction, game_id: int, winner: int):
    # Tarkista voittajan numero
    if winner not in (1, 2):
        return await interaction.response.send_message("Virhe: voittajan tulee olla **1** (team1) tai **2** (team2).", ephemeral=True)

    # Hae peli
    game = await bot.db.get_game(game_id)
    if not game:
        return await interaction.response.send_message("Peliä ei löytynyt annetulla ID:llä.", ephemeral=True)

    # Oletus: ei ylikirjoitusta
    overwrite = False

    # Meitsillä oikeus ylikirjottaa :3
    if interaction.user.id == 97687348396953600:
        overwrite = True

    # Päivitä tietokanta
    try:
        team1, team2 = await bot.db.set_winner(game_id, winner, overwrite=overwrite)
    except ValueError as e:
        return await interaction.response.send_message(str(e), ephemeral=True)

    # Ota varmistus
    await backup_db()

    await interaction.response.send_message(f"Pelin `{game_id}` voittajaksi asetettu **team{winner}** {'(ylikirjoitettu)' if overwrite else ''}.")

@setwinner_cmd.autocomplete("game_id")
async def setwinner_game_id_autocomplete(interaction: discord.Interaction, current: str):
    ids = await bot.db.get_recent_game_ids(10)
    if current:
        ids = [gid for gid in ids if str(gid).startswith(current)]
    return [app_commands.Choice(name=f"Peli {gid}", value=gid) for gid in ids]


@bot.tree.command(name="pstats", description="Näytä pelaajan tilastot ja sijoitukset embedinä")
@app_commands.describe(user="Valinnainen: käyttäjä, jonka tilastoja katsotaan")
async def pstats_cmd(interaction: discord.Interaction, user: Optional[discord.User] = None):
    target = user or interaction.user
    data = await bot.db.get_player(target.id)
    if not data:
        return await interaction.response.send_message("Ei tilastoja vielä.", ephemeral=True)

    gp = data["games_played"]
    w  = data["wins"]
    wr = (w / gp * 100) if gp else 0.0

    # Sijoitukset + kokonaismäärä
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

@bot.tree.command(name="dstatus", description="Näytä jonon/draftin status")
async def ds_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)

    # ---- Jono-embed (nimet, ei tägäystä) ----
    lines = []
    for i, uid in enumerate(st.queue, start=1):
        name = await get_display_name(interaction, uid)
        lines.append(f"{i}. {name}")
    desc = "\n".join(lines) if lines else "_(tyhjä)_"

    q_emb = discord.Embed(
        title=f"Jono ({len(st.queue)}/{QUEUE_SIZE})",
        description=desc,
        color=EMBED_COLOR_PRIMARY
    )
    q_emb.set_footer(text=EMBED_FOOTER_TEXT)

    # Readycheck-lisätiedot
    if st.readycheck_active:
        not_ready = [u for u in st.queue if u not in st.ready_users]
        if not_ready:
            nr_names = [await get_display_name(interaction, u) for u in not_ready]
            q_emb.add_field(name="Ei vielä valmiita", value=", ".join(nr_names), inline=False)
        q_emb.add_field(name="Readycheck", value=f"päällä ({READYCHECK_SECONDS}s)", inline=False)
        return await interaction.response.send_message(embed=q_emb)

    # Draftin aikana: näytä jono-embed + joukkue-embed (nimet, ei tägäystä)
    if st.draft_active:
        await interaction.response.defer(thinking=False)

        t1 = [await get_display_name(interaction, u) for u in st.team1]
        t2 = [await get_display_name(interaction, u) for u in st.team2]

        teams_emb = discord.Embed(title="Valitut joukkueet", color=EMBED_COLOR_PRIMARY)
        teams_emb.add_field(name="Team 1:", value=("\n".join(t1) if t1 else "-"), inline=True)
        teams_emb.add_field(name="Team 2:", value=("\n".join(t2) if t2 else "-"), inline=True)
        teams_emb.set_footer(text=EMBED_FOOTER_TEXT)

        await interaction.followup.send(embed=q_emb)
        return await interaction.followup.send(embed=teams_emb)

    # Ei readycheckiä eikä draftia: pelkkä jono-embed
    return await interaction.response.send_message(embed=q_emb)


@bot.tree.command(name="top10", description="Eniten pelejä pelanneet (Top 10)")
async def top10_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("games_played", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    lines = []
    for i, (uid, _, gp, wins) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        wr = (wins / gp * 100) if gp else 0.0
        lines.append(f"{i}. {name} / {gp}")

    emb = discord.Embed(title="Eniten pelejä pelanneet (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

@bot.tree.command(name="winners", description="Eniten pelejä voittaneet (Top 10)")
async def winners_cmd(interaction: discord.Interaction):
    rows = await bot.db.leaderboard("wins", 10)
    if not rows:
        return await interaction.response.send_message("Ei dataa vielä.", ephemeral=True)

    lines = []
    for i, (uid, wins, gp, _) in enumerate(rows, start=1):
        name = await get_display_name(interaction, uid)
        wr = (wins / gp * 100) if gp else 0.0
        lines.append(f"{i}. {name} / {wins} ({wr:.1f}%)")

    emb = discord.Embed(title="Eniten pelejä voittaneet (Top 10)", color=EMBED_COLOR_PRIMARY, description="\n".join(lines))
    emb.set_footer(text=EMBED_FOOTER_TEXT)
    await interaction.response.send_message(embed=emb)

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
    st.team1.clear(); st.team2.clear(); st.pick_pool.clear(); st.pick_index = 0
    await interaction.response.send_message("Jono ja draft-tila nollattu.")
    
"""@bot.tree.command(name="filltest", description="(Testi) täytä jono feikkipelaajilla; readycheck oikeille ja kapteenit oikeista")
async def filltest_cmd(interaction: discord.Interaction):
    assert interaction.guild_id
    st = bot.get_state(interaction.guild_id)
    uid = interaction.user.id

    # Lisää kutsuja jonoon jos puuttuu
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

    # Jos jono on täynnä eikä readycheck ole päällä, käynnistä readycheck.
    # Feikit merkitään valmiiksi heti; oikeat käyttäjät laittavat /r.
    if len(st.queue) >= QUEUE_SIZE and not st.readycheck_active and not st.draft_active:
        st.readycheck_active = True
        st.ready_users = set()
        st.ready_users.update(st.fake_users)  # feikit valmiiksi automaattisesti
        real_mentions = " ".join(mention(u) for u in st.queue if u not in st.fake_users)
        await interaction.followup.send(
            "**Testimoodi:** Feikkipelaajat merkitty valmiiksi.\n"
            f"{real_mentions}\n"
            f"Oikeat pelaajat: kirjoittakaa **/r** {READYCHECK_SECONDS} sekunnin sisällä."
        )
        await start_ready_timer(interaction, st)
        st.ready_task = asyncio.create_task(ready_timeout_run(interaction, st))
"""
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
