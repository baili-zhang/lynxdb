FROM openjdk:17

RUN mkdir data lib logs

COPY ./config /usr/local/lynxdb/config
COPY ./lynxdb-script/* /usr/local/lynxdb/
COPY ./lynxdb-server/target/lynxdb-server-2022.12.17-snapshot.jar /usr/local/lynxdb/lib/lynxdb-server-2022.12.17-snapshot.jar

CMD [ "./start-server.sh" ]