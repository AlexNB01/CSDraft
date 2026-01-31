# CSDraft Bot

A feature-rich Discord bot for organizing Counter-Strike draft matches with automated team selection, statistics tracking, and voice channel management.

[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Discord.py](https://img.shields.io/badge/discord.py-2.0+-blue.svg)](https://github.com/Rapptz/discord.py)

## Features

A simple Discord bot for organizing CS2 inhouse matches with:

- Automated queue, readycheck, and captain draft flow
- Player stats tracking (wins, captain picks, first/last pick counts)
- Pick turn breakdowns with average pick round stats
- Elo ratings with leaderboards
- Winrate comparisons between players
- Optional automatic voice channel moves
- Opt-out controls for captain selection

Please don't let this ruin your friendships too much. :3

## Quick Start

### Prerequisites

- Python 3.10 or higher
- Discord Bot Token ([Create one here](https://discord.com/developers/applications))
- Discord Server with voice channels

### Required configuration

1. **Configure voice channels** (Optional)

   Edit `main.py` lines 24-27 with your Discord voice channel IDs:

   ```python
   TEAM1_VOICE_CHANNEL_ID = <id>
   TEAM2_VOICE_CHANNEL_ID = <id>
   VOICE_LOBBY_CHANNEL_ID = <id>
   ```

   To get channel IDs: Enable Developer Mode in Discord → Right-click channel → Copy ID

Key settings in `main.py`:

```python
QUEUE_SIZE = 10                    # Players needed for a draft
READYCHECK_SECONDS = 120           # Readycheck timeout (2 minutes)
PICK_TIMEOUT_SECONDS = 45          # Captain pick timeout
AUTO_VOICE_CHANNELS = True         # Enable automatic voice channel moves
EMBED_COLOR_PRIMARY = 0x29377e     # Embed color (hex)
```

### MatchZy + CSSharp integration

This bot can generate MatchZy configs and trigger MatchZy via RCON. Configure these environment variables (for example in a `.env` file):

```
CS2_RCON_HOST=127.0.0.1
CS2_RCON_PORT=27015
CS2_RCON_PASSWORD=your_rcon_password
CS2_MATCH_CONFIG_FORMAT=matchzy
CS2_MATCH_CONFIG_DIR=./match_configs
CS2_MATCH_CONFIG_TARGET_DIR=C:\CSServer\game\csgo\addons\counterstrikesharp\plugins\MatchZy\match_configs
CS2_MATCH_CONFIG_RCON_DIR=addons/counterstrikesharp/plugins/MatchZy/match_configs
CS2_MATCH_CONFIG_URL_BASE=http://127.0.0.1:8080/match_configs
CS2_MATCH_PLUGIN_START_CMD=matchzy_loadmatch
CS2_MATCH_PLUGIN_START_CMDS=matchzy_loadmatch
CS2_SERVER_START_SCRIPT=C:\CSServer\start.bat
CS2_SERVER_START_WORKDIR=C:\CSServer
CS2_MATCH_RESULTS_DB=C:\CSServer\server\game\csgo\addons\counterstrikesharp\plugins\MatchZy\matchzy.db
```

- `CS2_MATCH_CONFIG_TARGET_DIR` copies the generated JSON into your MatchZy config folder.
- `CS2_MATCH_CONFIG_RCON_DIR` should be the path (relative to `csgo/`) used by `matchzy_loadmatch`.
- `CS2_MATCH_CONFIG_URL_BASE` is required for `matchzy_loadmatch_url` and must be a valid `http(s)` URL that serves the generated JSON files. You can also use `{filename}` as a placeholder (e.g. `http://127.0.0.1:8080/match_configs/{filename}`).
- If `CS2_MATCH_CONFIG_RCON_DIR` is empty and the target path contains `csgo/`, the bot will auto-derive the relative RCON path.
- `CS2_MATCH_PLUGIN_START_CMDS` allows the bot to try multiple MatchZy commands until one works (comma-separated).
- `CS2_SERVER_START_SCRIPT` starts the CS server automatically (optional).
- `CS2_MATCH_RESULTS_DB` points at MatchZy's SQLite database so the bot can detect finished matches.

## Commands

### Player Commands

| Command | Aliases | Description |
|---------|---------|-------------|
| `/add` | `!add`, `!dad`, `!ad`, etc. | Join the draft queue |
| `/rm` | `!rm`, `!remove` | Leave the queue |
| `/r` | `!r`, `!ready` | Ready up during readycheck (or click button) |
| `/pstats [user]` | `!pstats` | View player statistics and rankings |
| `/pickstats [user]` | `!pickstats` | View pick turn counts and average pick round |
| `/winrate <user>` | `!winrate`, `!wr` | Compare your winrate against another player |
| `/elo [user]` | `!elo` | View Elo rating and ranking |
| `/dstatus` | `!dstatus` | Check current queue/draft status |
| `/pick <number>` | `!pick`, `!p` | Captain: Pick a player by number |
| `/nocaptain` | `!nocaptain` | Opt out of being randomly selected as captain |
| `/allowcaptain` | `!allowcaptain` | Re-allow captain selection |

### Leaderboard Commands

| Command | Description |
|---------|-------------|
| `/top10` | Players with most games |
| `/topelo` | Top 10 Elo rankings |
| `/winners` | Players with most wins (sorted by win rate) |
| `/captains` | Players who've captained most |
| `/thinkids` | Players picked first most often |
| `/fatkids` | Players picked last most often |

### Admin Commands

| Command | Description |
|---------|-------------|
| `/setwinner <game_id> <1\|2\|0>` | Set winning team for a game (0 = draw) |
| `/setdraw <game_id>` | Mark game as a draw |
| `/reset` | Clear queue and draft state (requires Manage Server) |
| `/recalcelo` | Recalculate Elo from all recorded games |
| `/filltest` | Fill queue with test players (dev only) |

## Flow for queue

### 1. Queue Phase

Players use `/add` to join the queue. When 10 players are queued, readycheck begins automatically.

### 2. Readycheck

Players have 120 seconds to click the ready button or type `/r`. Players who don't ready up are removed from the queue.

### 3. Draft Phase

- Two captains are selected from eligible players (defaults to 20+ games played; if fewer than two meet the threshold, falls back to anyone who has not opted out)
- First captain is chosen at random from the eligible pool
- Second captain is chosen from the remaining eligible pool with the closest Elo rating to the first captain (ties randomized)
- Captains take turns picking players using `/p <number>`
- Pick order: Team1 → Team2 → Team1 → Team2 → Team1 → Team2 → Team2
- Each captain has 45 seconds per pick (auto-pick if timeout)
- Last player automatically assigned to Team 1

### 4. Game Time

- Teams are displayed in an embed
- If enabled, players are automatically moved to team voice channels after 15 seconds
- Game ID is generated for stat tracking

### 5. Post-Game

- Typically captain sets winner with `/setwinner <game_id> <1|2>` or `/setdraw <game_id>`
- Stats are updated automatically
- Players moved back to lobby voice channel after 15 seconds

## Database Schema

The bot uses SQLite with the following tables:

### players

- `user_id` - Discord user ID (primary key)
- `games_played` - Total games participated in
- `wins` - Total wins
- `captain_wins` - Wins as captain
- `captain_count` - Times selected as captain
- `first_pick_count` - Times picked first
- `last_pick_count` - Times picked last

### games

- `id` - Auto-incrementing game ID
- `guild_id` - Discord server ID
- `team1` - JSON array of Team 1 user IDs
- `team2` - JSON array of Team 2 user IDs
- `winner` - 1, 2, 0 (draw), or NULL (unset)
- `created_at` - Timestamp

### ratings

- `user_id` - Discord user ID (primary key)
- `rating` - Elo rating
- `elo_games` - Elo games counted

### rating_history

- `game_id` - Game ID
- `user_id` - Discord user ID
- `pre_rating` - Elo before the match
- `post_rating` - Elo after the match
- `delta` - Elo change
- `created_at` - Timestamp

### captain_opt_out

- `user_id` - Discord user ID (primary key)

Database file: `draftbot.sqlite3`
Backups: `backups/` directory (last 10 kept)

## Elo system

Elo ratings are calculated per match using the team average ratings, then applied to every player on the team:

- **Initial rating:** 1000
- **Expected score:** `1 / (1 + 10^((team_b_avg - team_a_avg) / 400))`
- **Delta:** `BASE_MATCH_DELTA * (score - expected)` where score is 1.0 for a win, 0.0 for a loss, and 0.5 for a draw
- **Caps:** win/loss deltas are clamped to ±30; draw deltas are clamped to ±5
- **Tracking:** every rating change is saved to `rating_history` with pre/post ratings and the delta

### Testing Mode

Use `/filltest` to automatically fill the queue with fake players for testing:

- Adds 9 test players to complete the queue
- Test players auto-ready during readycheck
- Test players skipped for voice channel operations
- Only accessible to developer (hardcoded user ID)

### State Management

Each Discord server (guild) maintains independent state:

- Queue contents
- Readycheck status
- Draft progress
- Team compositions

## Credits

**Developer:** AlexNB01

**Contributor:** TomG-FIN

**Built with:** discord.py, aiosqlite, Python 3.10+

### CSDraft by AlexNB01
