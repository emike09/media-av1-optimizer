@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%Media2AV1Queue-Interactive.ps1"

where pwsh >nul 2>nul
if errorlevel 1 (
    echo PowerShell 7 ^(pwsh.exe^) was not found in PATH.
    pause
    exit /b 1
)

if "%~1"=="" (
    echo Drag one or more video files or folders onto this .bat to queue them.
    pause
    exit /b 0
)

pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
pause
exit /b %errorlevel%
