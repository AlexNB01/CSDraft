@echo off

REM ------------------------------------------
REM  Discord Draft Bot Launcher (Windows)
REM ------------------------------------------

REM Aktivoi virtuaaliympäristö (luo se jos ei ole)
if not exist .venv (
    echo [*] Luodaan virtuaaliympäristö...
    python -m venv .venv
)

call .venv\Scripts\activate.bat

REM Asennetaan tarvittavat kirjastot
echo [*] Asennetaan riippuvuudet (discord.py ja aiosqlite)...
pip install -U discord.py aiosqlite python-dotenv >nul

echo.
echo [*] Käynnistetään Discord Draft Bot...
echo.

python main.py

pause