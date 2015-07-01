#!/bin/bash

DBHost=$1
DB=$2

TotalTx=10000
DEST=target
res=stats
processes=( 1 2 3 4 5 6 7 8 9 10 )
threadsInc=( 5 10 15 20 25 30 35 40 45 50 55 60 65 70 )
JAR=$DEST/clusterj-test-1.0-SNAPSHOT-jar-with-dependencies.jar

if [ -d $res ]; then
  rm -rf $res
fi

mkdir $res

for thInc in ${threadsInc[@]}
do

  for p in ${processes[@]}
  do
    java -jar $JAR -dbHost $DBHost -schema $DB -clean
    i=$p
    PIDS=()
    while [ $i -gt 0 ]
     do
       let index=($p-$i)
       let to=$index*$thInc

        if [ ! -d $res/$p ]; then
          mkdir $res/$p
        fi

       echo "START Process $P numThreads $thInc ThreadOffset $to"
       java -jar $JAR -dbHost $DBHost -schema $DB -numThreads $thInc -threadOffset $to -totalTx $TotalTx -statsFile $res/$p/$thInc-$p-$i &
       PIDS+=($!)
       echo "PIDS ${PIDS[@]}"
       let i=$i-1
    done

    for pid in ${PIDS[@]}
    do
      wait $pid
    done

  done

done

echo "Aggregate Stats"

let lpi=${#processes[@]}-1
let lthi=${#threadsInc[@]}-1

java -cp $JAR io.hops.clusterj.test.StatsAggregator $res ${processes[0]} ${processes[$lpi]} ${threadsInc[0]} ${threadsInc[$lthi]}
cd $res
gnuplot *.gnu

