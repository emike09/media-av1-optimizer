@echo off
REM ===========================================================================
REM Media2AV1Queue
REM
REM   Drag video files or folders onto this file  ->  they get encoded
REM   Double-click it with nothing selected       ->  a menu opens
REM
REM There is only one script now. Everything -- encoding, the diagnostics, the
REM library scans, the loudness tools -- lives in Media2AV1Queue.ps1 and is
REM reachable from the menu.
REM ===========================================================================
setlocal
set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%Media2AV1Queue.ps1"

where pwsh >nul 2>nul
if errorlevel 1 (
    echo.
    echo PowerShell 7 ^(pwsh.exe^) was not found.
    echo Install it from https://aka.ms/powershell and run this again.
    echo.
    pause
    exit /b 1
)

if not exist "%PS1%" (
    echo.
    echo Could not find Media2AV1Queue.ps1 next to this file:
    echo   %PS1%
    echo.
    pause
    exit /b 1
)

if "%~1"=="" (
    pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
) else (
    pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
)

pause
exit /b %errorlevel%
