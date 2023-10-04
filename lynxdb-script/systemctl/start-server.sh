#!/bin/bash
java -Dlynxdb.baseDir=/root/lynxdb-v2023.10.5-snapshot/\
     -Xmx256m -Xms256m\
     -XX:+UseZGC\
     -jar /root/lynxdb-v2023.10.5-snapshot/lib/lynxdb-server-2023.10.5-snapshot.jar