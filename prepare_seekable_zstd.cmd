@echo off
setlocal
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0prepare_seekable_zstd.ps1" %*
exit /b %ERRORLEVEL%
