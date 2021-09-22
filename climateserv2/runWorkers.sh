#!/bin/sh

HEADWORKERS=3
WORKERSPERHEAD=8

python="/cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/python"
echo python
rootdir=/cserv2/django_app/ClimateSERV2/climateserv2/
BASEIPCDIR="ipc:///cserv2/tmp/servir/"
MAINQUEUEINPUT=${BASEIPCDIR}Q1/input
MAINQUEUEOUTPUT=${BASEIPCDIR}Q1/output
PIDNAME=/tmp/pid
cd ${rootdir}
export PYTHONPATH=${PYTHONPATH}:${rootdir}

launch() {
	echo 'python: ' + $python
	$python db/DBMDbprocessing.py
	$python file/fileutils.py /cserv2/tmp/servir/Q1 input
	
	$python file/fileutils.py /cserv2/tmp/servir/Q1 output
	
	echo "starting the Head Workers"
	##################Start Input Queue########################################
	$python zmqconnected/ArgProxyQueue.py ${MAINQUEUEINPUT} ${MAINQUEUEOUTPUT} &
	echo $! > ${PIDNAME}
	
	##################Start Head Workers and subordinate workers###############
	for i in  $(seq 1 $HEADWORKERS);
	do
		echo 'starting Head Processer'$i
		
		HEADNAME=HEAD${i}
		echo "Starting Head Worker Named:" $HEADNAME
		$python file/fileutils.py /cserv2/tmp/servir/${HEADNAME} q1in
		$python file/fileutils.py /cserv2/tmp/servir/${HEADNAME} q1out
		$python file/fileutils.py /cserv2/tmp/servir/${HEADNAME} q2in
		$python file/fileutils.py /cserv2/tmp/servir/${HEADNAME} q2out
		HEADQUEUEONEINPUT=${BASEIPCDIR}${HEADNAME}'/q1in'
		HEADQUEUEONEOUTPUT=${BASEIPCDIR}${HEADNAME}'/q1out'
		HEADQUEUETWOINPUT=${BASEIPCDIR}${HEADNAME}'/q2in'
		HEADQUEUETWOOUTPUT=${BASEIPCDIR}${HEADNAME}'/q2out'
		$python zmqconnected/ZMQCHIRPSHeadProcessor.py ${HEADNAME} ${MAINQUEUEOUTPUT} ${HEADQUEUEONEINPUT} ${HEADQUEUETWOOUTPUT} &
		echo $! >> ${PIDNAME}
		$python zmqconnected/ArgProxyQueue.py ${HEADQUEUEONEINPUT} ${HEADQUEUEONEOUTPUT} &
		value=$!
		echo ${value} >> ${PIDNAME}
		for  j in $(seq 1 $WORKERSPERHEAD);
		do
			WORKERNAME=W${j}${HEADNAME}
			echo "Starting Worker: $WORKERNAME"
			$python zmqconnected/ZMQCHIRPSDataWorker.py ${WORKERNAME} ${HEADQUEUEONEOUTPUT} ${HEADQUEUETWOINPUT}  &
			echo $! >> ${PIDNAME}
		done
		$python zmqconnected/ArgProxyQueue.py ${HEADQUEUETWOINPUT} ${HEADQUEUETWOOUTPUT} &
		echo $! >> ${PIDNAME}
		
	done
	$python file/filePermissions.py /cserv2/tmp/servir/Q1/input
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
  status)
    if [ -f $PIDNAME ]; then
      echo running
    else
      echo failed
      exit 1
    fi
    ;;
  *)
    echo "Usage: $0 {start|stop|restart}"
esac
