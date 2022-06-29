#kafka server
install kafka
$ ./start-kafka-server1.sh
$ ./start-kafka-server2.sh

# install ib jar
mvn install:install-file \
-Dfile=/home/steven/git/ats-ib-rest/TwsApi.jar \
-DgroupId=com.ib \
-DartifactId=tws-api \
-Dversion=9.73.01-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true


# ats-ib-rest

# start kafka server
$ ./start-kafka-server1.sh

$ ./start-kafka-server2.sh

# start ib-rest
$ ./start-ats-ib-rest.sh


# connect ib
curl -X POST http://localhost:8201/connection/connect

# disconnect ib
curl -X POST http://localhost:8201/connection/disconnect
