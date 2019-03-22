#!/usr/bin/env bash


error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"


JAVA="$JAVA_HOME/bin/java"
BASE_DIR=$(dirname $0)/..
CLASSPATH=.:${BASE_DIR}/conf:${BASE_DIR}/libs/*:${CLASSPATH}

#gc log path
if [ ! -d @gc.log.path@ ]; then
  mkdir @gc.log.path@ -p
fi

#log path
if [ ! -d @log.path@ ]; then
  mkdir @log.path@ -p
fi


JAVA_OPT="${JAVA_OPT} -server @memory.conf@"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:@gc.log.path@/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintAdaptiveSizePolicy"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100m"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -XX:+AlwaysPreTouch"
JAVA_OPT="${JAVA_OPT} -XX:MaxDirectMemorySize=4g"
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages -XX:-UseBiasedLocking"
JAVA_OPT="${JAVA_OPT} @jmx.conf@"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"


nohup $JAVA ${JAVA_OPT} com.globalegrow.datatransfer.server.DataTransferServerApplication $@ > /dev/null 2>&1 &