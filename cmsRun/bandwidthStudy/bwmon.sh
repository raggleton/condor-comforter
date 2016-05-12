#!/bin/bash
#
# usage: bwmon PID

IN=0; OUT=0; TIME=0

get_traffic() {
    t=`awk '/eth1:/ { printf("%s,%d,%d\n",strftime("%s"),$2,$10); }' < /proc/$1/net/dev`
    IN=${t#*,}; IN=${IN%,*}
    OUT=${t##*,};
    TIME=${t%%,*};
}
echo "TIME,DTIME,IN,DIN,OUT,DOUT"
get_traffic $1
while true
do
    _IN=$IN; _OUT=$OUT; _TIME=$TIME
    get_traffic $1
    echo "$TIME,$(( $TIME - $_TIME )),$IN,$(( $IN - $_IN )),$OUT,$(( $OUT - $_OUT))"
    sleep 1
done
