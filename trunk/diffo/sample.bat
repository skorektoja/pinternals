@echo off
call diffo -tx 10 -th 20 -d diffo.db -s SID -x http://host:50000 -u login -p password start addHost refresh finish 
