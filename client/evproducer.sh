#!/bin/bash
SERVER_IP=$1
evproducer_pid=$(nohup /opt/mpmt-readout/evproducer --host $SERVER_IP --disable-rc > /dev/null 2>&1 & echo $!)
echo "Started evproducer with PID $evproducer_pid and server IP $SERVER_IP"
exit
