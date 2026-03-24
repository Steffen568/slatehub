@echo off
REM SlateHub — Windows Task Scheduler Setup
REM Right-click this file and "Run as administrator" once.
REM All SlateHub refresh tasks will be created automatically.

SET SCRIPT="%~dp0refresh_all.py"

echo.
echo Creating SlateHub scheduled tasks...
echo.

REM ── QUICK REFRESH every 15 minutes ───────────────────────────────────────────
REM Schedule + lineups + weather only (~30 sec). Catches lineup confirmations
REM and weather updates continuously throughout the day.
REM /sc MINUTE /mo 15 = every 15 minutes
schtasks /create /tn "SlateHub Quick Refresh" /tr "py -3.12 %SCRIPT% --quick" /sc MINUTE /mo 15 /f
echo [OK] Quick refresh (schedule + weather) .. every 15 minutes

REM ── MORNING (9:00 AM daily) ───────────────────────────────────────────────────
REM Full pull: last night bullpen/logs + DK slates/salaries
schtasks /create /tn "SlateHub Morning" /tr "py -3.12 %SCRIPT% --morning" /sc daily /st 09:00 /f
echo [OK] Morning refresh ..................... 9:00 AM daily

REM ── POST-GAME (11:30 PM daily) ────────────────────────────────────────────────
REM Final bullpen pitch counts after west coast games finish
schtasks /create /tn "SlateHub Post-Game" /tr "py -3.12 %SCRIPT% --postgame" /sc daily /st 23:30 /f
echo [OK] Post-game .......................... 11:30 PM daily

REM ── DAILY STATS (7:00 AM daily) ──────────────────────────────────────────────
REM Season stats — FanGraphs/Savant process last night's games overnight.
REM By 7am the previous day's stats are ready. Takes ~15 min.
schtasks /create /tn "SlateHub Daily Stats"  /tr "py -3.12 %SCRIPT% --stats"  /sc daily /st 07:00 /f
echo [OK] Daily stats ........................ 7:00 AM daily

schtasks /create /tn "SlateHub Daily Splits" /tr "py -3.12 %SCRIPT% --splits" /sc daily /st 07:30 /f
echo [OK] Daily splits ....................... 7:30 AM daily

echo.
echo ============================================================
echo  All tasks created. Summary:
echo.
echo  Every 15 min  Schedule + lineups + weather (all day)
echo  7:00 AM       Daily season stats (FanGraphs + Savant)
echo  7:30 AM       Daily splits upload
echo  9:00 AM       Full morning pull + DK slates/salaries
echo  11:30 PM      Post-game bullpen + game logs
echo.
echo  To verify: Win+R, type taskschd.msc, look for "SlateHub"
echo  To run manually: right-click any task and select "Run"
echo ============================================================
echo.
pause
