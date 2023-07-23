#!/bin/bash
java -Dlynxdb.baseDir=/root/lynxdb-v2023.7.23-alpha/\
     -Xmx256m -Xms256m\
     -XX:+UseZGC\
     -jar /root/lynxdb-v2023.7.23-alpha/lib/lynxdb-server-2023.7.23-alpha.jar