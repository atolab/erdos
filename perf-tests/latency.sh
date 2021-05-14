#!/bin/bash


source venv/bin/activate

if test "$#" -ne 3; then
    echo "USAGE:"
    echo "    latency.sh <out-dir> <transport> <N> "
    echo ""
    exit 1
fi

WD=$1
TEST="python3 python/examples/latency.py"
TRANSPORT=$2
N=$3

SIZE="8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304"
# 8388608 16777216"

DATE=`eval date "+%F-%T"`
DATA_PATH=$WD
mkdir -p  $DATA_PATH

for s in $SIZE; do
    {
        TS=`eval date "+%F-%T"`
        timeout 60 script -qc "$TEST -p $s -n $N -t $TRANSPORT" > $DATA_PATH/$s.txt &
        echo "[$TS]: Testing latency for $s bytes"
        S_PID=$!
        TS=`eval date "+%F-%T"`
        echo "[$TS]: Started Test (PID = $S_PID)"
        wait $S_PID
        sleep 1

    }
    TS=`eval date "+%F-%T"`
    echo "[$TS]: Test completed"
    sleep 1
done

TIME=$TS

echo "kind,scenario,test,name,size,seq_num,rtt" > $DATA_PATH/rtt-$TRANSPORT-$TIME.csv

cat $DATA_PATH/*.txt >> $DATA_PATH/rtt-$TRANSPORT-$TIME.csv


deactivate