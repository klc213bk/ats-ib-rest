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
