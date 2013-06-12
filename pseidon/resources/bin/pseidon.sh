###########################
#
# Start the pseidon
# e.g. pseidon.sh
#!/usr/bin/env bash


abspath=$(cd ${0%/*} && echo $PWD/${0##*/})
BIN_HOME=`dirname $abspath`

PSEIDON_HOME=/opt/pseidon

export CONF_DIR=$PSEIDON_HOME/conf

#source environment variables

# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
#echo "run java in $JAVA_HOME"
JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
echo "Error: JAVA_HOME is not set."
exit 1
fi

JAVA=$JAVA_HOME/bin/java


if [ -z $JAVA_HEAP ]; then
export JAVA_HEAP="-Xmx2048m"
fi

# check envvars which might override default args
# CLASSPATH initially contains $CONF_DIR
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# so that filenames w/ spaces are handled correctly in loops below
# add libs to CLASSPATH.
for f in $PSEIDON_HOME/lib/*.jar; do
CLASSPATH=${CLASSPATH}:$f;
done

CLIENT_CLASS="pseidon.core"

CLASSPATH=$CONF_DIR:$CONF_DIR/META-INF:$CLASSPATH

$JAVA -XX:MaxDirectMemorySize=2048M -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=6 -XX:NewRatio=3 -XX:+DisableExplicitGC $JAVA_HEAP $JAVA_OPTS -Djava.library.path="$STREAMS_HOME/lib/native/Linux-amd64-64/" -classpath "$CLASSPATH" $CLIENT_CLASS $@
