HOME="/app"
TEST_PATH="/app/tests/"
VALGRIND="valgrind --tool=callgrind --collect-jumps=yes --simulate-cache=yes --combine-dumps=yes "
NETSNMP_PATH_TEST=$HOME"/tests/"

CALLGRIND_OUT=$TEST_PATH"callgrind.out.final"
OUTPUT="--callgrind-out-file=/app/tests/callgrind.out.final "

APP_SNMPTRAPD=$HOME"/apps/.libs/snmptrapd -f -Lf /var/log/snmptrapd.log -c /app/tests/snmptrapd.conf"


### Warning: It is necessary to comment these line If this script is run repeatedly
export MIBS=ALL
export SNMPCONFPATH=/app/tests/snmpd.conf
#cp /app/mibs/* /usr/local/share/snmp/mibs 

mkdir -p /usr/local/share/snmp/mibs
cp -a /app/mibs /usr/local/share/snmp/mibs

echo "==============================================="
echo "Valgrind start"
echo "==============================================="
echo "LD_LIBRARY_PATH=/app/apps/.libs/" $VALGRIND$OUTPUT$APP_SNMPTRAPD
LD_LIBRARY_PATH=/app/apps/.libs:/app/agent/.libs:/app/snmplib/.libs:/app/agent/helpers/.libs/ $VALGRIND$OUTPUT$APP_SNMPTRAPD &

sleep 20

# kafkacat:
KAFKACAT_PATH="/kafkacat"
JSON_OUT=$TEST_PATH'kafka_json_messages1.log'

echo "==============================================="
echo "Kafkacat. Get the json messages. output: 'json_out_kafka.log' "
echo $KAFKACAT_PATH/kafkacat -C -o end -c 2 -D "\n" -b 172.16.238.11 -t rb_snmp
echo "==============================================="
$KAFKACAT_PATH/kafkacat -C -o beggining -c 3 -D "\n" -b 172.16.238.11 -t rb_snmp > $JSON_OUT &

## TRAPS (diffences tests.)

TRAP1="/app/apps/snmptrap -v 2c -c redborder localhost '' NET-SNMP-EXAMPLES-MIB::netSnmpExampleHeartbeatNotification netSnmpExampleHeartbeatRate i 123456"

TRAP2="/app/apps/snmptrap -v 1 -c redborder localhost 1.2.3.4.5.6 192.193.194.193 6 99 55 1.11.12.13.14.15  s teststring23w222"

# TRAP with empty value. The json file does not have this element "netSnmpExampleHeartbeatName" and the log file shows "Discarding empty line."
#/app/apps/snmptrap -v 2c -c redborder localhost '' NET-SNMP-EXAMPLES-MIB::netSnmpExampleHeartbeatNotification netSnmpExampleHeartbeatRate i 123456 netSnmpExampleHeartbeatName s ""

echo "==============================================="
echo $TRAP1
echo $TRAP2
echo "/app/apps/snmptrap -v 2c -c redborder localhost '' NET-SNMP-EXAMPLES-MIB::netSnmpExampleHeartbeatNotification netSnmpExampleHeartbeatRate i 123456 netSnmpExampleHeartbeatName s """
echo "==============================================="

$TRAP1
$TRAP2
/app/apps/snmptrap -v 2c -c redborder localhost '' NET-SNMP-EXAMPLES-MIB::netSnmpExampleHeartbeatNotification netSnmpExampleHeartbeatName s ""

sleep 30
if ps aux | grep -v "grep" | grep "valgrind" 1> /dev/null
then
    echo "Valgrind is running. Send SIGINT to valgrind"
    for i in `ps aux | grep -v "grep"|grep "valgrind"|awk '{print $2}'|uniq`; do /usr/bin/kill -SIGINT $i; done
else
   echo "Valgrind is stopped"
fi

## net-snmp sends json message to kafka. Here, kafkacat reads these json messages.
sleep 5
if ps aux | grep -v "grep" | grep "kafkacat" 1> /dev/null
then
    echo "kafkacat is running. Kill it."
    for i in `ps aux | grep -v "grep"|grep "kafkacat"|awk '{print $2}'|uniq`; do /usr/bin/kill -SIGINT $i; done
else
   echo "kafkacat is stopped"
fi

## Coverage system:

CALLGRIND_PATH="/callgrind/callgrind_coverage/cg_coverage"
CALLGRIND_OUT=$TEST_PATH"callgrind-out-1.log"
$CALLGRIND_PATH $TEST_PATH"callgrind.out.final" $HOME"/apps/snmptrapd_kafka.c" > $CALLGRIND_OUT
echo "==============================================="
echo $CALLGRIND_PATH $TEST_PATH"callgrind.out.final" $HOME"/apps/snmptrapd_kafka.c"
echo "==============================================="
echo "Callgrind file: " $CALLGRIND_OUT

head -n 3 /app/tests/callgrind-out-1.log

echo "== json messages received in kafka server =="
cat $JSON_OUT


# test1
PYCHECKJSON="/usr/bin/checkjson.py"
JSON_CHECK_TEMPLATE=$TEST_PATH"template-1.json"
#DEBUG=" -d"
DEBUG=""
echo "==============================================="
echo $PYCHECKJSON -t $JSON_CHECK_TEMPLATE -j $JSON_OUT$DEBUG
echo "==============================================="
$PYCHECKJSON -t $JSON_CHECK_TEMPLATE -j $JSON_OUT$DEBUG

