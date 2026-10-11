@echo off
title InfluxDB v2.7 - PTIT IoT (B23DCCN522)
echo =========================================================
echo    KHOI DONG INFLUXDB 2.7 - TIME-SERIES DATABASE
echo    Hoc vien: INT14149 - PTIT
echo    Sinh vien: Bui Cam Ly - B23DCCN522
echo    Web UI: http://localhost:8086
echo    Bucket: iot_telemetry  ^| Org: ptit_iot
echo =========================================================
cd /d "%~dp0"
bin\influxd.exe
pause
