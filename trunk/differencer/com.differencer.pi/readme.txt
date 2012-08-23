Welcome!

1) Install plugin as usual or simply put in dropins directory of your eclipse.

2) Define two environment variables:
DIFFO_LIB=<path to diffo*.jar with subfolder lib/ for sqlite and tagsoup libraries>

for example from https://code.google.com/p/pinternals/
diffo_api.jar,
diffo.jar,
lib/sqlite-jdbc-3.7.2.jar
lib/tagsoup-1.2.1.jar

DIFFERENCER_LIB=<path to SAP libaries>

for example from following locations (simply put them together)
.<SID>/J00/j2ee/j2eeclient/signed/sap.com~tc~bl~guidgenerator~impl.jar,
.<SID>/J00/j2ee/cluster/bin/ext/tc.httpclient/lib/private/sap.com~tc~clients~http~all.jar,
.<SID>/J00/j2ee/j2eeclient/sap.com~tc~exception~impl.jar,
.<SID>/J00/j2ee/j2eeclient/signed/sap.com~tc~logging~java~impl.jar,
.<SID>/J00/j2ee/cluster/bin/ext/com.sap.lcr.api.cimclient/lib/sap.com~tc~sld~lcrclient_lib.jar,
.<SID>/J00/j2ee/cluster/bin/ext/tc~sld~sldclient_sda/lib/sap.com~tc~sld~sldclient_lib.jar,
.<SID>/J00/j2ee/cluster/bin/ext/com.sap.aii.ibtransportclient/lib/tc~aii~ibtransportclient_api.jar,
.<SID>/J00/j2ee/cluster/bin/ext/com.sap.aii.util.misc/lib/tc~aii~util_api.jar 

 * you can pass these variables to Eclipse using -vmargs -DDIFFO_LIB=<> -DDIFFERENCER_LIB=<>