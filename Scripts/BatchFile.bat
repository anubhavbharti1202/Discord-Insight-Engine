@echo off 
REM Run the Python script batch file 
call "C:\Path\To\Your\run_script.bat" 
REM Wait for the script to complete (adjust if needed) 
timeout /t 3 /nobreak 
REM Open the Power BI dashboard file 
start "" "C:\Path\To\Your\dashboard.pbix" 
