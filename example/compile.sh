#!/bin/bash

# gcc server.c -o server -lraft -lhmac -I /usr/local/include/ -L /usr/local/lib/ -luv 

pushd /root/leases/libsgxlease
make
cp libsgxlease.a /usr/local/lib/
popd

pushd /root/leases/safeleader
make
popd

gcc server.c -o sv /root/leases/safeleader/node.o /root/leases/safeleader/emmc-client.o -I /usr/local/include/ -I ../../leases/safeleader -L /usr/local/lib/ -luv -lraft -lhmac -lcrypto -lssl -lsgxlease -lIPSec_MB