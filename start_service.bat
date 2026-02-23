@echo off
cd /d C:\shopify_updater
python app\service_runner.py >> data\logs\service.log 2>&1
