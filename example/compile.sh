#!/bin/bash

# gcc server.c -o server -lraft -lhmac -I /usr/local/include/ -L /usr/local/lib/ -luv 

pushd /root/leases/libfakelease
make
cp libfakelease.a /usr/local/lib/
popd

pushd /root/leases/safeleader
# see this Makefile, change node -> node_dummy if want to use python dummy emmc
make
popd
# the output is either node.o or node_dummy.o


gcc server.c -o sv /root/leases/safeleader/node_dummy.o /root/leases/safeleader/emmc-client.o -I /usr/local/include/ -I ../../leases/safeleader -L /usr/local/lib/ -luv -lraft -lhmac -lcrypto -lssl -lfakelease -lIPSec_MB