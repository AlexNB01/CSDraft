# CSDraft Bot

A feature-rich Discord bot for organizing Counter-Strike draft matches with automated team selection, statistics tracking, and voice channel management.

[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Discord.py](https://img.shields.io/badge/discord.py-2.0+-blue.svg)](https://github.com/Rapptz/discord.py)

## Features

A simple Discord bot for organizing CS2 inhouse matches with:

- Automated queue, readycheck, and captain draft flow
- Map draft after team selection
- Player stats tracking (wins, captain picks, first/last pick counts)
- Pick turn breakdowns with average pick round stats
- Elo ratings with leaderboards
- Winrate comparisons between players
- Optional automatic voice channel moves
- Optional CS2 server start with competitive ruleset and server IP output
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

### Optional CS2 server settings

Set environment variables to let the bot launch a CS2 server after the map draft:

- `CS2_SERVER_COMMAND` - Shell command template for starting the server. Use `{map}` to insert the chosen map.
- `CS2_SERVER_IP` - Fallback IP address to display if the command does not print an IP.

Example:

```bash
export CS2_SERVER_COMMAND="./start_cs2_server.sh --map \"{map}\" --ruleset competitive"
export CS2_SERVER_IP="203.0.113.42:27015"
```

Windows example using the included `cs2server.bat` (expects server files in `C:\CSServer`):

```bat
set CS2_SERVER_COMMAND=cs2server.bat "{map}"
set CS2_SERVER_IP=203.0.113.42:27015
```

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
| `/banmap <map>` | `!banmap` | Captain: Ban a map during map draft |
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
- Map draft begins after teams are locked in
- Map pool: Ancient, Anubis, Dust II, Inferno, Mirage, Nuke, Overpass
- Captains take turns banning maps until one remains

### 4. Game Time

- Teams are displayed in an embed
- Captains ban maps until one remains
- Bot launches a CS2 server on the remaining map (if configured)
- Server IP is posted in the channel
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
