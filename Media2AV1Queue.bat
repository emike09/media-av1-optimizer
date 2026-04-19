@echo off
setlocal enabledelayedexpansion
set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%Media2AV1Queue.ps1"

where pwsh >nul 2>nul
if errorlevel 1 (
    echo PowerShell 7 ^(pwsh.exe^) was not found in PATH.
    pause
    exit /b 1
)

rem FIX #16: Build a quoted argument list from %* manually so that paths containing
rem spaces survive the hand-off to pwsh correctly. %* does not re-quote individual
rem arguments that Explorer already quoted, but building the list explicitly with
rem %~1 / shift handles edge cases and is safer across all Windows versions.
set "ARGS="
:arg_loop
if "%~1"=="" goto run
set "ARGS=!ARGS! "%~1""
shift
goto arg_loop

:run
pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%" -- !ARGS!
exit /b %errorlevel%
