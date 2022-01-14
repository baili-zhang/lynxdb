@echo off
title start-server.bat
java -XX:MaxDirectMemorySize=5g -Xmx2g -Xms2g -jar lib/server-1.1-SNAPSHOT.jar