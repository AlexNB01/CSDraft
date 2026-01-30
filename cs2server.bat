@echo off
setlocal ENABLEEXTENSIONS

set "SERVER_DIR=C:\CSServer"
set "MAP_NAME=%~1"

if "%MAP_NAME%"=="" (
  set "MAP_NAME=Mirage"
)

set "MAP_CODE="
if /I "%MAP_NAME%"=="Ancient" set "MAP_CODE=de_ancient"
if /I "%MAP_NAME%"=="Anubis" set "MAP_CODE=de_anubis"
if /I "%MAP_NAME%"=="Dust II" set "MAP_CODE=de_dust2"
if /I "%MAP_NAME%"=="Inferno" set "MAP_CODE=de_inferno"
if /I "%MAP_NAME%"=="Mirage" set "MAP_CODE=de_mirage"
if /I "%MAP_NAME%"=="Nuke" set "MAP_CODE=de_nuke"
if /I "%MAP_NAME%"=="Overpass" set "MAP_CODE=de_overpass"

if "%MAP_CODE%"=="" set "MAP_CODE=%MAP_NAME%"

pushd "%SERVER_DIR%"
call start.bat -dedicated -console +game_type 0 +game_mode 1 +map "%MAP_CODE%"
popd

endlocal
