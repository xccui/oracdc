#!/bin/sh
CONNECT_HOME=/kafka/connect
LIB_HOME=$CONNECT_HOME/lib
# Add  PGJDBC-NG jar's
CLASSPATH=$LIB_HOME/HikariCP-3.4.1.jar:$LIB_HOME/netty-transport-native-kqueue-4.1.42.Final.jar:$LIB_HOME/pgjdbc-ng-0.8.3.jar:$LIB_HOME/spy-0.8.3.jar
# Add Oracle jar's
CLASSPATH=$LIB_HOME/ojdbc8.jar:$LIB_HOME/ucp.jar:$LIB_HOME/oraclepki.jar:$LIB_HOME/osdt_core.jar:$LIB_HOME/osdt_cert.jar:${CLASSPATH}
# Add misc jar's
CLASSPATH=$LIB_HOME/commons-math3-3.6.1.jar:${CLASSPATH}
export CLASSPATH
connect-standalone -daemon /kafka/config/connect-avro-standalone.properties $CONNECT_HOME/config/sink-testapps-wsh.properties
