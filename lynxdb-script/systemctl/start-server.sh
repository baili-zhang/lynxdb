#!/bin/bash
java -Dlynxdb.baseDir=/root/lynxdb-v2024.5.3-alpha/\
     -Xmx256m -Xms256m\
     -XX:+UseZGC\
     -jar /root/lynxdb-v2024.5.3-alpha/lib/lynxdb-server-2024.5.3-alpha.jar