#!/bin/bash
evproducer_pid=$(nohup /opt/mpmt-readout/evproducer --host 172.16.24.107 --disable-rc > /dev/null 2>&1 & echo $!)
exit
