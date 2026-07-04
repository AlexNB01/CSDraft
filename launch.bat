@echo off

REM ------------------------------------------
REM  Discord Draft Bot Launcher (Windows)
REM ------------------------------------------

REM Etsitaan Python-tulkki PATH:sta (python.exe tai py-launcher)
set "PYCMD="
where python >nul 2>nul
if not errorlevel 1 set "PYCMD=python"
if not defined PYCMD (
    where py >nul 2>nul
    if not errorlevel 1 set "PYCMD=py -3"
)
if not defined PYCMD (
    echo [!] Pythonia ei loydy PATH:sta.
    echo     Asenna Python 3.10+ osoitteesta https://www.python.org/downloads/
    echo     ja varmista, etta "Add python.exe to PATH" on valittuna asennuksen aikana.
    pause
    exit /b 1
)

REM Luodaan virtuaaliymparisto, jos sita ei viela ole
if not exist .venv (
    echo [*] Luodaan virtuaaliymparisto... :3
    %PYCMD% -m venv .venv
)

if not exist .venv\Scripts\python.exe (
    echo [!] Virtuaaliymparisto jai luomatta - .venv\Scripts\python.exe puuttuu.
    echo     Tarkista etta Python 3.10+ on asennettu oikein ja etta sinulla
    echo     on kirjoitusoikeudet tahan kansioon, ja yrita sitten uudelleen.
    pause
    exit /b 1
)

set "VENV_PY=.venv\Scripts\python.exe"

REM Asennetaan tarvittavat kirjastot suoraan venv:n omalla Pythonilla
REM (ei riipu siita, etta "pip" loytyy erikseen PATH:sta)
echo [*] Tarkistetaan/asennetaan riippuvuudet :3
"%VENV_PY%" -m pip install -U -r requirements.txt
if errorlevel 1 (
    echo [!] Riippuvuuksien asennus epaonnistui. Tarkista virheet ylla.
    pause
    exit /b 1
)

echo.
echo [*] Kaynnistetaan Discord Draft Bot... :3
echo.

"%VENV_PY%" main.py

pause
