#!/bin/bash

#systemctl start postgresql-12

# export path to zmq, pqxx, boost, ToolFrameworkCore and ToolDAQFramework
cd /opt/middleman
. Setup.sh

# setup database environmental variables
# (probably not required as they're overridden in the config file anyway)
export PG_COLOR=always
export PGHOST=/tmp
export PGPORT=5432
if [ -z "${PGUSER}" ]; then
	export PGUSER=postgres
fi
export PGDATABASE=daq
#export PGDATA=/var/lib/pgsql/data

# wait up to 30s for the postgres server to be ready
#pg_isready -t 30

# running the middleman in a loop enables reloading/recovery
# but we need to provide some means to fully terminate.
# the configuration specifies a quit file; if found, we quit
# default is '$PWD/quit', but check the config for user a override
STOPFILE="stop"
QUITFILE="quit"
while read -r -a LINE; do
	#echo "next line is '${LINE}'"
	if [ "${LINE[0]}" == "quitfile" ]; then
		#echo "quitfile: ${LINE[1]}";
		QUITFILE="${LINE[1]}";
	fi;
	if [ "${LINE[0]}" == "stopfile" ]; then
		#echo "stopfile: ${LINE[1]}";
		STOPFILE="${LINE[1]}";
	fi;
done < <(cat ./configfiles/middleman/StopQuitFileConfig)

if [ -f ${QUITFILE} ]; then
	rm ${QUITFILE}
fi
if [ -f ${STOPFILE} ]; then
	rm ${STOPFILE}
fi

# run the middleman
while [ true ]; do
	echo -n "starting middleman at " >> middleman_runs.log
	date >> middleman_runs.log
	DATETIME=$(date +%s)
	./main ./configfiles/middleman/ToolChainConfig 2> >(tee middleman_errors_${DATETIME}.log)
	echo -n "middleman exited with code $? at " >> middleman_runs.log
	date >> middleman_runs.log
	sleep 1
	rm -f ${STOPFILE}
	if [ -f ${QUITFILE} ]; then
		rm $QUITFILE
		break;
	fi
done

