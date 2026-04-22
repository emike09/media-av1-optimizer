@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%Media2AV1Queue.ps1"

where pwsh >nul 2>nul
if errorlevel 1 (
    echo PowerShell 7 ^(pwsh.exe^) was not found in PATH.
    exit /b 1
)

:run
if "%~1"=="" (
    pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
) else (
    pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%" -- %*
)
exit /b %errorlevel%
