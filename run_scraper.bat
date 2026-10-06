@echo off
REM =====================================================================
REM  COLLOKY SCRAPER - run in the background (Windows)
REM ---------------------------------------------------------------------
REM  Double-click this file. It opens a MINIMIZED window that runs the
REM  scraper; you can keep using the computer. Progress is written to
REM  the "logs" folder next to this file.
REM
REM  Put this .bat in the SAME folder as colloky_scraper.py and
REM  colloky_schema.sql.
REM =====================================================================

cd /d "%~dp0"

REM --- Python to use --------------------------------------------------
REM If "python" is not recognized, replace it with the full path of your
REM Anaconda Python, for example:
REM   set PYTHON_EXE=C:\Users\YOUR_USER\anaconda3\python.exe
set PYTHON_EXE=python

REM --- PostgreSQL credentials (only if they are not already set) ------
REM If in Jupyter you set the password with an environment variable,
REM uncomment and fill these lines:
REM set POSTGRES_USER=postgres
REM set POSTGRES_PASSWORD=your_password
REM set POSTGRES_HOST=127.0.0.1
REM set POSTGRES_PORT=5432
REM set POSTGRES_DATABASE=postgres

REM --- Resume an interrupted snapshot (optional) ----------------------
REM set COLLOKY_REANUDAR_SNAPSHOT=2026-10-06 08:47:00

set PYTHONIOENCODING=utf-8

start "Colloky scraper" /min cmd /c ""%PYTHON_EXE%" colloky_scraper.py"
