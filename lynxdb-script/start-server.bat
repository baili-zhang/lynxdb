@echo off
title start-lynxdb-server.bat
java -Xmx2g -Xms2g -XX:+UseZGC -jar lib/lynxdb-server-2024.5.3-alpha.jar