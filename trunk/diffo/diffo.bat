@echo off
set hd=-XX:+HeapDumpOnOutOfMemoryError -XX:+UseCompressedOops

rem agent is JMX / profiling tool
set agent=-agentpath:"C:\Program Files (x86)\YourKit Java Profiler 11.0.7\bin\win64\yjpagent.dll"

rem with SAX
set lib=./lib/sqlite-jdbc-3.7.2.jar;./lib/tagsoup-1.2.1.jar;./lib/commons-cli-1.2.jar
set sld=saplib/sap.com~tc~sld~lcrclient_lib.jar;saplib/sap.com~tc~logging~java~impl.jar;saplib/sap.com~tc~clients~http~all.jar;saplib/sap.com~tc~sld~sldclient_lib.jar

java -Xms32m -Xmx96m -esa -ea -cp .;%lib%;%sld%;diffo.jar com/pinternals/diffo/Main %*
rem java -Xms32m -Xmx96m -cp %cp%;%sld% com/pinternals/diffo/Main %*
