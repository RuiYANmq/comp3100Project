#!/bin/bash
# to kill multiple runaway processes, use 'pkill runaway_process_name'
# For the Java implementation, use the following format: ./tests1.sh your_client.class [-n]
configDir="./configs"
diffLog="stage1diff.log"
if [ ! -d $configDir ]; then
	echo "No $configDir found!"
	exit
fi

if [ -f $configDir/$diffLog ]; then
	rm $configDir/*.log
fi

if [ $# -lt 1 ]; then
	echo "Usage: $0 your_client [-n]"
	exit
fi

if [ ! -f $1 ]; then
	echo "No $1 found!"
	echo "Usage: $0 your_client [-n]"
	exit
fi

if [ -f $configDir/$diffLog ]; then
	rm $configDir/$diffLog
fi

trap "kill 0" EXIT

conf=configs/config_simple$2.xml
	echo "running the reference implementation (./ds-client)..."
	sleep 1
    ./ds-server -c $conf -v brief > $conf.ref.log&
    sleep 1
    ./ds-client -a bf
	
	echo "running your implementation ($1)..."
	sleep 2
    ./ds-server -c $conf -v brief > $conf.your.log&
	sleep 1
	if [ -f $1 ] && [[ $1 == *".class" ]]; then
		java $(sed 's/\.class//' <<< $1)
	else
		./$1
	fi
	sleep 1
	diff $conf.ref.log $conf.your.log > $configDir/temp.log
	if [ -s $configDir/temp.log ]; then
		echo NOT PASSED!
	else
		echo PASSED!
	fi
	echo ============
	sleep 1
	cat $configDir/temp.log >> $configDir/$diffLog

echo "testing done! check $configDir/$diffLog"

