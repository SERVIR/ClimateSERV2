#!/bin/sh

HEADWORKERS=3
WORKERSPERHEAD=8

python="C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe"
echo python
rootdir=D:\\ClimateSERV2\\climateserv2\\
BASEIPCDIR="inproc://D:/tmp/"
MAINQUEUEINPUT=${BASEIPCDIR}Q1/input
MAINQUEUEOUTPUT=${BASEIPCDIR}Q1/output
PIDNAME=/tmp/pid
cd ${rootdir}
export PYTHONPATH=${PYTHONPATH}:${rootdir}

launch() {
	echo 'python: ' + $python
	"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" db/bddbprocessing.py
	"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/fileutils.py /tmp/servir/Q1 input
	
	"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/fileutils.py /tmp/servir/Q1 output
	
	echo "starting the Head Workers"
	##################Start Input Queue########################################
	"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" zmqconnected/ArgProxyQueue.py ${MAINQUEUEINPUT} ${MAINQUEUEOUTPUT} &
	echo $! > ${PIDNAME}
	
	##################Start Head Workers and subordinate workers###############
	for i in  $(seq 1 $HEADWORKERS);
	do
		echo 'starting Head Processer'$i
		
		HEADNAME=HEAD${i}
		echo "Starting Head Worker Named:" $HEADNAME
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/fileutils.py /tmp/servir/${HEADNAME} q1in
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/fileutils.py /tmp/servir/${HEADNAME} q1out
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/fileutils.py /tmp/servir/${HEADNAME} q2in
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/fileutils.py /tmp/servir/${HEADNAME} q2out
		HEADQUEUEONEINPUT=${BASEIPCDIR}${HEADNAME}'/q1in'
		HEADQUEUEONEOUTPUT=${BASEIPCDIR}${HEADNAME}'/q1out'
		HEADQUEUETWOINPUT=${BASEIPCDIR}${HEADNAME}'/q2in'
		HEADQUEUETWOOUTPUT=${BASEIPCDIR}${HEADNAME}'/q2out'
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" zmqconnected/ZMQCHIRPSHeadProcessor.py ${HEADNAME} ${MAINQUEUEOUTPUT} ${HEADQUEUEONEINPUT} ${HEADQUEUETWOOUTPUT} &
		echo $! >> ${PIDNAME}
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" zmqconnected/ArgProxyQueue.py ${HEADQUEUEONEINPUT} ${HEADQUEUEONEOUTPUT} &
		value=$!
		echo ${value} >> ${PIDNAME}
		for  j in $(seq 1 $WORKERSPERHEAD);
		do
			WORKERNAME=W${j}${HEADNAME}
			echo "Starting Worker: $WORKERNAME"
			"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" zmqconnected/ZMQCHIRPSDataWorker.py ${WORKERNAME} ${HEADQUEUEONEOUTPUT} ${HEADQUEUETWOINPUT}  &
			echo $! >> ${PIDNAME}
		done
		"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" zmqconnected/ArgProxyQueue.py ${HEADQUEUETWOINPUT} ${HEADQUEUETWOOUTPUT} &
		echo $! >> ${PIDNAME}
		
	done
	"C:\\ProgramData\\Anaconda3\\envs\\ClimateSERV2\\python.exe" file/filePermissions.py /tmp/servir/Q1/input
}

start() {
	if [ -f $PIDNAME ] && kill -0 $(cat $PIDNAME); then
    	echo 'Service already running' >&2
    	return 1
  	fi
  	echo 'Starting service' >&2
	launch	
  	echo 'Service started' >&2
}

stop() {
	if [ ! -f "$PIDNAME" ] || ! kill -0 $(cat "$PIDNAME"); then
    	echo 'Service not running' >&2
    	return 1
  	fi
  	echo 'Stopping service' >&2
  	kill -15 $(cat "$PIDNAME") && rm -f "$PIDNAME"
 	 echo 'Service stopped' >&2
}



case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  *)
    echo "Usage: $0 {start|stop|restart}"
esac