#!/bin/bash

if [ "$1" == "" ]; then
	echo "empty 1"
	exit
fi

target_id=$1
target_pid=`netstat -plten | grep 900$target_id | awk '{print $9}' | sed -e 's/\/.*$//'`

mkdir -p /sys/fs/cgroup/net_cls/block
echo 42 > /sys/fs/cgroup/net_cls/block/net_cls.classid

iptables -D OUTPUT 1
iptables -A OUTPUT -m cgroup --cgroup 42 -j DROP

if [ "$2" == "unblock" ]; then
	echo "UNBLOCKING $target_id ($target_pid)"
	echo $target_pid > /sys/fs/cgroup/net_cls/tasks
	exit
else
	echo "Blocking $target_id ($target_pid)"
	echo $target_pid > /sys/fs/cgroup/net_cls/block/tasks	
fi

