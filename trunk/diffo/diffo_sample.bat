@echo off
diffo -s DEV -x http://xi_dev:50000 -u pireport -p passwd start refresh finish
diffo -s QLT -x http://xi_qas:50000 -u pireport -p passwd start refresh finish
