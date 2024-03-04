#!/bin/bash
java -Dlynxdb.baseDir=/root/lynxdb-v1.0.0-alpha/\
     -Xmx256m -Xms256m\
     -XX:+UseZGC\
     -jar /root/lynxdb-v1.0.0-alpha/lib/lynxdb-server-1.0.0-alpha.jar