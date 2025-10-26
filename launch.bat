@echo off

REM ------------------------------------------
REM  Discord Draft Bot Launcher (Windows)
REM ------------------------------------------

REM Aktivoi virtuaaliympäristö (luo se jos ei ole)
if not exist .venv (
    echo [*] Luodaan virtuaaliymparisto... :3
    python -m venv .venv
)

call .venv\Scripts\activate.bat

REM Asennetaan tarvittavat kirjastot
echo [*] Asennetaan riippuvuudet :3
pip install -U discord.py aiosqlite python-dotenv >nul

echo.
echo [*] Kaynnistetaan Discord Draft Bot... :3
echo.

python main.py

pause