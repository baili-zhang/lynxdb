@echo off
title start-lynxdb-server.bat
java -Xmx2g -Xms2g -XX:+UseZGC -jar lib/lynxdb-server-2023.6.19-alpha.jar