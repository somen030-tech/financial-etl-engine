

@echo off
setlocal

cd /d D:\financial-etl-engine

if not exist data\logs mkdir data\logs

echo. >> data\logs\scheduled_pipeline_output.log
echo ================================================== >> data\logs\scheduled_pipeline_output.log
echo [%date% %time%] Scheduled pipeline starting >> data\logs\scheduled_pipeline_output.log

.venv\Scripts\python.exe src\18_recovery_runner.py >> data\logs\scheduled_pipeline_output.log 2>&1

set "PIPELINE_EXIT_CODE=%ERRORLEVEL%"

echo [%date% %time%] Final exit code: %PIPELINE_EXIT_CODE% >> data\logs\scheduled_pipeline_output.log

exit /b %PIPELINE_EXIT_CODE%
