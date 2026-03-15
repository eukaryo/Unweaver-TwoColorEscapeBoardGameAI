@echo off
setlocal

set "_SCRIPT_DIR=%~dp0"
set "_PAUSE=1"

if /I "%~1"=="--no-pause" (
    set "_PAUSE=0"
    shift
)

"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%_SCRIPT_DIR%build_windows_baseline_player.ps1" %*
set "_RC=%ERRORLEVEL%"

echo.
if "%_RC%"=="0" (
    echo Build succeeded.
) else (
    echo Build failed with exit code %_RC%.
)

if "%_PAUSE%"=="1" pause
exit /b %_RC%
