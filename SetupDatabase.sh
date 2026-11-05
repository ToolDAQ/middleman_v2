#!/bin/bash
set +x
systemctl status &>/dev/null
USE_SYSTEMD=$?
set -e
if [ ${USE_SYSTEMD} -eq 0 ]; then
	echo "running database as systemd unit"
else
	echo "running database via pg_ctl"
fi
# get location of DB
DEFAULT_LOCATION=0
if [ -z "${PGROOT}" ]; then
	PGROOT=/var/lib/pgsql
	DEFAULT_LOCATION=1
fi
export PGDATA=${PGROOT}/data

# rocky9 does not have postgres18 in default repos yet,
# but after installing from postgres' repo, the install location is not in the default PATH...
export PATH=/usr/pgsql-18/bin:$PATH
export LD_LIBRARY_PATH=/usr/pgsql-18/lib:$LD_LIBRARY_PATH
echo "PATH+=:/usr/pgsql-18/bin" >> SetupDB.sh
echo "LD_LIBRARY_PATH+=:/usr/pgsql-18/lib" >> SetupDB.sh

# only take action on first run
if [ -f /.DBSetupDone ]; then
	
	# systemd version for baremetal
	if [ ${USE_SYSTEMD} -eq 0 ]; then
		# note no [ ] in following check
		if ! systemctl is-active --quiet postgresql-18; then
			sudo systemctl start postgresql-18
		fi
	else
		# pg_ctl version for containers
		STATUS=$(sudo -u postgres $(which pg_ctl) -D ${PGDATA} status &> /dev/null; echo $?)
		if [ ${STATUS} -eq 3 ]; then
			if [ -f /var/run/postgresql/.s.PGSQL.5432.lock ]; then
				echo "removing stale lockfile"
				sudo rm -f /var/run/postgresql/.s.PGSQL.5432.lock
			fi
			echo "running pg_ctl start"
			sudo -u postgres $(which pg_ctl) start -D ${PGDATA} -s -o "-p 5432" -w -t 300
		fi
	fi
	
	# run scripts to pre-populate
	find ./scripts -type f -name '*.sh' -exec sh -c "if [ ! -f {}.done ]; then {} && touch {}.done; fi" \;
	
	exit 0;
fi

export LC_ALL=C
echo "Initialising postgresql cluster at location ${PGROOT}. Is this OK?"
select result in OK Change Cancel; do
	case $result in
		OK)
			break;
			;;
		Change)
			read -p "new path: " PGROOT
			if [ $? -ne 0 ]; then
				exit 0;
			fi
			export PGDATA=${PGROOT}/data
			# this cannot be the correct way to do this...
			echo -e "Initialising postgresql cluster at location ${PGROOT}. Is this OK?\n1) OK\n2) Change\n3) Cancel"
			;;
		Cancel)
			exit 0;
			;;
		*)
			echo "enter 1, 2 or 3"
			;;
	esac
done

# initdb requires a clean directory, so remove any existing
if [ -d ${PGROOT} ] && [ ! -n "$(find ${PGROOT} -prune -empty -type d 2>/dev/null)" ]; then
	echo "${PGROOT} is not empty - any existing database will be completely cleared. Continue?";
	select result in OK Cancel; do
		case $result in
			OK)
				break;
				;;
			Cancel)
				exit 0;
				;;
			*)
				echo "enter 1 or 2"
				;;
		esac
	done
fi

rm -rf ${PGROOT}
mkdir -p ${PGROOT}
chown -R postgres:postgres ${PGROOT}
cd ${PGROOT}
# FIXME
# --waldir=/todo/replication
# locale='C' for faster string matching
sudo -u postgres $(which initdb) --data-checksums  --locale='C' ${PGDATA}

# set it up to listen on all network interfaces, rather than (by default) localhost only
echo "listen_addresses = '*'" | sudo -u postgres tee -a ${PGDATA}/postgresql.conf

echo "Starting postgres server"
if [ ${USE_SYSTEMD} -eq 0 ]; then
	# if not using the default install location, we need to modify the systemctl file
	if [ ${DEFAULT_LOCATION} -ne 1 ]; then
		#systemctl edit --stdin postgresql <<-EOF        # requires systemd v256
		SYSTEMD_EDITOR=tee systemctl edit postgresql-18 <<-EOF
		[Service]
		Environment=PGDATA=${PGDATA}
		EOF
	fi
	
	# systemd version
	sudo systemctl enable --now postgresql-18
else
	# container version
	sudo mkdir -p /var/run/postgresql && sudo chown -R postgres /var/run/postgresql
	sudo -u postgres $(which pg_ctl) start -D ${PGDATA} -s -o "-p 5432" -w -t 300
	
	#echo "registering database to start on boot"
	#echo " sudo -u postgres $(which pg_ctl) start -D ${PGDATA} -s -o \"-p 5432\" -w -t 300;" >> /etc/rc.local
fi

echo "creating root database user"
sudo -u postgres createuser -s root
echo "creating 'daq' database"
sudo -u postgres psql -c "CREATE DATABASE daq WITH owner=root;"

# set timezone to UTC
psql -ddaq -c "ALTER DATABASE daq SET TIME ZONE 'UTC';"

# also clear out directory for SSD tablespace if specified
if [ ! -z ${SSD_PGROOT} ]; then
	if [ -d ${SSD_PGROOT} ] && [ ! -n "$(find ${SSD_PGROOT} -prune -empty -type d 2>/dev/null)" ]; then
		echo "${SSD_PGROOT} (SSD tablespace) is not empty - any existing database will be completely cleared. Continue?";
		select result in OK Cancel; do
			case $result in
				OK)
					break;
					;;
				Cancel)
					exit 0;
					;;
				*)
					echo "enter 1 or 2"
					;;
			esac
		done
	fi
	
	rm -rf ${SSD_PGROOT}
	mkdir -p ${SSD_PGROOT}
	chown -R postgres:postgres ${SSD_PGROOT}
	
	psql -ddaq -c "CREATE TABLESPACE ssd_tablespace LOCATION '${SSD_PGROOT}';"
	# use it for temporary tables
	psql -ddaq -c "ALTER SYSTEM SET temp_tablespaces TO ssd_tablespace;"
	# we'll also use it for indexes and other things, e.g. last day's monitoring
	export FAST_TABLESPACE=" TABLESPACE ssd_tablespace;"
	
fi

# setup pg_partman
psql -ddaq -c "CREATE SCHEMA partman;"
psql -ddaq -c "CREATE EXTENSION pg_partman SCHEMA partman;"
psql -ddaq -c "CREATE ROLE partman_maintainer WITH LOGIN;"
psql -ddaq -c "GRANT ALL ON SCHEMA partman TO partman_maintainer;"
psql -ddaq -c "grant root to partman_maintainer;" # root being the owner of our tables. for better security we could use a dedicated role instead
psql -ddaq -c "GRANT ALL ON ALL TABLES IN SCHEMA partman TO partman_maintainer;"
psql -ddaq -c "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA partman TO partman_maintainer;"
psql -ddaq -c "GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA partman TO partman_maintainer;"
psql -ddaq -c "GRANT TEMPORARY ON DATABASE daq to partman_maintainer;"
#psql -ddaq -c "GRANT CREATE ON DATABASE daq TO partman_maintainer;"

# we'll have partman detach older partitions and move them to a different schema
# actually we need to do downsampling, so have to do this manually
#psql -ddaq -c "CREATE SCHEMA old_tables;"
#psql -ddaq -c "GRANT ALL ON SCHEMA old_tables TO partman_maintainer;"

# FIXME temporary fix for bug remove when partman is updated
cd /opt/pg_partman && git checkout subpart_fix && cd -
psql -U postgres -d postgres -f /opt/pg_partman/fix_sub_partitioning.sql

# TODO FIXME To optimize storage and minimize wasted space due to alignment padding,
# it's advisable to arrange columns in the table definition from largest to smallest data type.

echo "creating users table"
psql -ddaq -c "CREATE TABLE users (user_id integer GENERATED ALWAYS AS IDENTIY PRIMARY KEY, username text NOT NULL, password_hash text NOT NULL, permissions jsonb);"

echo "creating index on users table"
psql -ddaq -c "CREATE UNIQUE INDEX user_name_idx ON users(LOWER(username));"

echo "creating base_config table"
# add more fields: retired by, on, reason?
psql -ddaq -c "CREATE TABLE base_config (config_id integer GENERATED ALWAYS AS IDENTIY PRIMARY KEY, time timestamp with time zone NOT NULL DEFAULT now(), name text NOT NULL, version int NOT NULL, description text NOT NULL, author_id integer NOT NULL references users(user_id), retired boolean NOT NULL DEFAULT FALSE, data jsonb NOT NULL);"

 # n.b. this index is doing double duty of enforcing unique {device:version} constraint and providing an ordered index
 # a unique(name,version) constraint uses an index to accomplish this under the hood, by doing it explicitly we can make the index ordered
echo "creating index on base_config table"
psql -ddaq -c "CREATE UNIQUE INDEX ON base_config (name, version DESC NULLS LAST)"

echo "creating autoincrement function for base_config version"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_base_config_ver"() returns "pg_catalog"."trigger" as $BODY$ begin PERFORM pg_advisory_xact_lock(hashtextextended(NEW.name, 0)); new.version = (select COALESCE(MAX(version)+1,1) from base_config where name=new.name); return NEW; end; $BODY$ LANGUAGE plpgsql VOLATILE;'
psql -ddaq -c 'CREATE TRIGGER trig_base_config_ver BEFORE insert ON base_config FOR EACH ROW EXECUTE PROCEDURE fn_base_config_ver();'

echo "creating runmode_config table"
# add more fields: retired by, on, reason?
psql -ddaq -c "CREATE TABLE runmode_config (config_id integer GENERATED ALWAYS AS IDENTIY PRIMARY KEY, time timestamp with time zone NOT NULL DEFAULT now(), name text NOT NULL, version int NOT NULL, description text NOT NULL, author_id INTEGER NOT NULL references users(user_id), retired boolean NOT NULL DEFAULT FALSE, data jsonb NOT NULL);"

echo "creating index on runmode_config table"
psql -ddaq -c "CREATE UNIQUE INDEX ON runmode_config (name, version DESC NULLS LAST)"

echo "creating autoincrement function for runmode_config version"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_runmode_config_ver"() returns "pg_catalog"."trigger" as $BODY$ begin PERFORM pg_advisory_xact_lock(hashtextextended(NEW.name, 0)); new.version = (select COALESCE(MAX(version)+1,1) from runmode_config where name=new.name); return NEW; end; $BODY$ LANGUAGE plpgsql VOLATILE;'
psql -ddaq -c 'CREATE TRIGGER trig_runmode_config_ver BEFORE insert ON runmode_config FOR EACH ROW EXECUTE PROCEDURE fn_runmode_config_ver();'

echo "creating run_info table"
psql -ddaq -c "CREATE TABLE run_info (run_number integer GENERATED ALWAYS AS IDENTIY PRIMARY KEY, start_time timestamp with time zone NOT NULL, stop_time timestamp with time zone, base_config_id int NOT NULL references base_config(config_id), runmode_config_id int NOT NULL references runmode_config(config_id), testing boolean NOT NULL, comments text NOT NULL, bad_devices json NOT NULL DEFAULT '{}');"

echo "creating devices table"
# more fields: created on, by? retirement cause?
psql -ddaq -c "CREATE TABLE devices (device_id integer GENERATED ALWAYS AS IDENTIY PRIMARY KEY, name TEXT NOT NULL, description TEXT, author_id INTEGER NOT NULL references users(id), created_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), retired_time TIMESTAMP WITH TIME ZONE DEFAULT NULL, retired_user_id INTEGER references users(user_id) DEFAULT NULL, retired boolean NOT NULL DEFAULT FALSE);"

# functional index to ensure no duplicates even ignoring case
echo "creating index on devices table"
psql -ddaq -c "CREATE UNIQUE INDEX dev_name_idx ON devices(LOWER(name));"

echo "creating device_config table"
# XXX IMPORTANT: VERSION 0 OF ALL DEVICE CONFIGURATIONS SHOULD BE DEVICE OFF
# references device by name not id... better with our config retrieval method as we get requests by name
psql -ddaq -c "CREATE TABLE device_config (id integer GENERATED ALWAYS AS IDENTIY PRIMARY KEY, time timestamp with time zone NOT NULL DEFAULT now(), device text references devices(name), version int NOT NULL, author_id INTEGER NOT NULL references users(user_id), description text NOT NULL, data json NOT NULL, retired_time TIMESTAMP WITH TIME ZONE DEFAULT NULL, retired_user_id INTEGER references users(user_id) DEFAULT NULL, retired BOOLEAN NOT NULL DEFAULT FALSE);"

echo "creating index on device_config table"
psql -ddaq -c "CREATE UNIQUE INDEX ON device_config (device, version DESC NULLS LAST)"

echo "creating autoincrement function for device_config version"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_devconfig_ver"() returns "pg_catalog"."trigger" as $BODY$ begin PERFORM pg_advisory_xact_lock(hashtextextended(NEW.device, 0)); new.version = (select COALESCE(MAX(version)+1,0) from device_config where device=new.device); return NEW; end; $BODY$ LANGUAGE plpgsql VOLATILE;'
psql -ddaq -c 'CREATE TRIGGER trig_devconfig_ver BEFORE insert ON device_config FOR EACH ROW EXECUTE PROCEDURE fn_devconfig_ver();'

echo "creating calibration table"
# FIXME change data to bytea? add created by?
psql -ddaq -c "CREATE TABLE calibration (time timestamp with time zone NOT NULL DEFAULT now(), name text NOT NULL, version int NOT NULL, description text NOT NULL, data json NOT NULL);"

echo "creating index on calibration table"
psql -ddaq -c "CREATE UNIQUE INDEX ON calibration (name, version DESC NULLS LAST)"

echo "creating autoincrement function for calibration version"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_calibration_ver"() returns "pg_catalog"."trigger" as $BODY$ begin PERFORM pg_advisory_xact_lock(hashtextextended(NEW.name, 0)); new.version = (select COALESCE(MAX(version)+1,0) from calibration where name=new.name); return NEW; end; $BODY$ LANGUAGE plpgsql VOLATILE;'
psql -ddaq -c 'CREATE TRIGGER trig_calibration_ver BEFORE insert ON calibration FOR EACH ROW EXECUTE PROCEDURE fn_calibration_ver();'

echo "creating logging table"
# we will not enforce logging device_ids referential integrity by choice: for performance, and because it's not important
psql -ddaq -c "CREATE TABLE logging (time timestamp with time zone NOT NULL DEFAULT now(), device_id integer NOT NULL, severity integer NOT NULL, message text NOT NULL, repeats integer NOT NULL DEFAULT 1) PARTITION BY RANGE (time);"

echo "creating indices on logging device name and message severity"
psql -ddaq -c "CREATE INDEX ON logging (device, time) ${FAST_TABLESPACE};"
#psql -ddaq -c "CREATE INDEX ON logging (device, severity, time) ${FAST_TABLESPACE};" # maybe useful if people filter on severity often...
#psql -ddaq -c "CREATE INDEX ON logging USING BRIN(time) ${FAST_TABLESPACE};"
#'ALTER TABLE logging ALTER COLUMN device SET STATISTICS 1000;' Default is 100, maximum is 10000.

echo "creating logging template table"
psql -ddaq -c "CREATE TABLE logging_template(LIKE logging);"

echo "creating logging partition parent and child tables"
psql -ddaq -c "SELECT partman.create_partition( p_parent_table:= 'public.logging', p_control := 'time', p_interval := '1 day', p_template_table:='public.logging_template');"

#echo "setting retention policy for logging table"
#psql -ddaq -c "UPDATE partman.part_config SET retention = '1 day', retention_schema = 'old_tables' WHERE parent_table = 'public.logging'";

echo "creating monitoring table"
psql -ddaq -c "CREATE TABLE monitoring (time timestamp with time zone NOT NULL DEFAULT now(), device_id integer NOT NULL, subject text NOT NULL, data json NOT NULL) PARTITION BY RANGE (time);"

echo "creating indices on monitoring device name and subject"
psql -ddaq -c "CREATE INDEX ON monitoring (device, time, subject) ${FAST_TABLESPACE};"
#psql -ddaq -c "CREATE INDEX ON monitoring USING BRIN(time) ${FAST_TABLESPACE};" # redundant with more useful B-Tree index on time

echo "creating monitoring template table"
psql -ddaq -c "CREATE TABLE monitoring_template(LIKE monitoring);"

echo "creating monitoring partition parent and child tables"
psql -ddaq -c "SELECT partman.create_partition( p_parent_table:= 'public.monitoring', p_control := 'time', p_interval := '1 day', p_template_table:='public.monitoring_template');"

#echo "setting retention policy for monitoring table"
#psql -ddaq -c "UPDATE partman.part_config SET retention = '1 day', retention_schema = 'old_tables' WHERE parent_table = 'public.monitoring'";

# note on device name index usage: postgres never uses indexes for case insensitive searches!
# we can create a functional index with 'lower(device)', but this only gets used if queries
# also use 'select lower(device) ... where lower(device) = ...'
# Perhaps better to just enforce that all device names is lowercase? postgres doesn't have a nice way to do this....
# we could use 'CHECK lower(device)=device' on device table to ensure names are lowercase, but not sure it helps..
#
# also, we may need to specify 'text_pattern_ops' after the field name for text pattern matching to use an index!
# e.g. "CREATE UNIQUE INDEX dev_name_idx ON devices(LOWER(name) text_pattern_ops);"
# https://www.www-old.bartlettpublishing.com/site/bartpub/blog/3/entry/329
# only if the locale is not 'C' - probbly not default. use `show lc_collate;` to find out.
# use '--no-locale' to initdb to set
# https://www.postgresql.org/docs/current/indexes-opclass.html
# N.B. B-tree indexes are only used for left-anchored searches: 'PMT%' not '%LED%'. Use GIN trigrams for latter.

echo "creating alarms table"
# FIXME ideally uid would be unique, but requires being part of partitioning column, so we apply to the template
# still use device name and do not enforce constraint
psql -ddaq -c "CREATE TABLE alarms ( uid integer GENERATED ALWAYS AS IDENTIY NOT NULL, status INTEGER DEFAULT 0, critical BOOLEAN NOT NULL, first_time TIMESTAMP WITH TIME ZONE NOT NULL, last_time TIMESTAMP WITH TIME ZONE NOT NULL, device TEXT NOT NULL, description TEXT NOT NULL, silence_user_id INTEGER references users(user_id) DEFAULT NULL, resolve_user_id INTEGER references users(user_id) DEFAULT NULL, expert_user_id INTEGER references users(user_id) DEFAULT NULL, resolve_time TIMESTAMP WITH TIME ZONE DEFAULT NULL, resolution_description TEXT DEFAULT NULL, event_counter INTEGER default 1 ) PARTITION BY RANGE (first_time);"

echo "creating indices on alarms table"
# uid is used in updates by webpage
psql -ddaq -c "CREATE INDEX ON alarms (uid) ${FAST_TABLESPACE};"
# for getting all alarms within a recent window... we should ensure these queries use last_time not first_time
psql -ddaq -c "CREATE INDEX ON alarms (last_time) ${FAST_TABLESPACE};"
# for queries that want to look up instances of alarms on a given device. we should ensure these use first_time, which seems more relevant? either or, it should match the WHERE clause.
psql -ddaq -c "CREATE INDEX ON alarms (device, first_time) WITH (deduplicate_items = on) ${FAST_TABLESPACE};" # deduplicate_items is on by default

echo "creating alarms template table"
psql -ddaq -c "CREATE TABLE alarms_template(LIKE alarms);"
psql -ddaq -c "ALTER TABLE alarms_template ADD PRIMARY KEY(uid);"

echo "creating alarms partition parent and child tables"
psql -ddaq -c "SELECT partman.create_partition( p_parent_table:= 'public.alarms', p_control := 'first_time', p_interval := '1 day', p_template_table:='public.alarms_template');"

echo "creating insert_alarm function"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "alarm_upsert"() RETURNS TRIGGER AS $BODY$ BEGIN UPDATE alarms SET event_counter=event_counter+1, last_time=NEW.first_time WHERE resolve_time IS NULL AND device=NEW.device AND description=NEW.description; IF FOUND THEN RETURN NULL; END IF; NEW.last_time = NEW.first_time; RETURN NEW; END; $BODY$ LANGUAGE plpgsql;'
psql -ddaq -c "CREATE TRIGGER trig_alarm_upsert BEFORE INSERT ON alarms FOR EACH ROW EXECUTE FUNCTION alarm_upsert();"

echo "creating global_alerts table"
# do we really want bytea or just JSON?
psql -ddaq -c "CREATE TABLE global_alerts ( time TIMESTAMP WITH TIME ZONE NOT NULL, name TEXT NOT NULL, payload bytea ) PARTITION BY RANGE (time);"

echo "creating indices on global_alerts table"
psql -ddaq -c "CREATE INDEX ON global_alerts (name) ${FAST_TABLESPACE};" # looking for specfic alerts
psql -ddaq -c "CREATE INDEX ON global_alerts (time) ${FAST_TABLESPACE};" # looking for all alerts over a timespan

echo "creating global_alerts template table"
psql -ddaq -c "CREATE TABLE global_alerts_template(LIKE global_alerts);"

echo "creating global_alerts partition parent and child tables"
psql -ddaq -c "SELECT partman.create_partition( p_parent_table:= 'public.global_alerts', p_control := 'time', p_interval := '1 day', p_template_table:='public.global_alerts_template');"

echo "creating rootplots table"
psql -ddaq -c "CREATE TABLE rootplots (time timestamp with time zone NOT NULL DEFAULT now(), name text NOT NULL, version int NOT NULL, data json NOT NULL, draw_options text NOT NULL DEFAULT '', lifetime int NOT NULL DEFAULT 5);"

psql -ddaq -c "CREATE UNIQUE INDEX ON rootplots (name, version DESC NULLS LAST)"

echo "creating autoincrement function for rootplots version"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_rootplot_ver"() returns "pg_catalog"."trigger" as $BODY$ begin PERFORM pg_advisory_xact_lock(hashtextextended(NEW.name, 0)); new.version = (select COALESCE(MAX(version)+1,0) from rootplots where name=new.name); DELETE FROM rootplots WHERE name=new.name AND version+lifetime < new.version; RETURN NEW; END; $BODY$ LANGUAGE plpgsql VOLATILE;'
psql -ddaq -c 'CREATE TRIGGER trig_rootplot_ver BEFORE insert ON rootplots FOR EACH ROW EXECUTE PROCEDURE fn_rootplot_ver();'

echo "creating plotlyplots table"
psql -ddaq -c "CREATE TABLE plotlyplots (time timestamp with time zone NOT NULL DEFAULT now(), name text NOT NULL, version int NOT NULL, data json NOT NULL, layout json NOT NULL DEFAULT '{}', traces json NOT NULL DEFAULT '[]', lifetime int NOT NULL DEFAULT 5);"

psql -ddaq -c "CREATE UNIQUE INDEX ON plotlyplots (name, version DESC NULLS LAST)"

echo "creating autoincrement function for plotlyplots version"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_plotlyplot_ver"() RETURNS "pg_catalog"."trigger" AS $BODY$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended(NEW.name, 0)); new.version = (SELECT COALESCE(MAX(version)+1,0) FROM plotlyplots WHERE name=new.name); DELETE FROM plotlyplots WHERE name=new.name AND version+lifetime < new.version; RETURN NEW; END; $BODY$ LANGUAGE plpgsql VOLATILE;'
psql -ddaq -c 'CREATE TRIGGER trig_plotlyplot_ver BEFORE insert ON plotlyplots FOR EACH ROW EXECUTE PROCEDURE fn_plotlyplot_ver();'

echo "creating event_display table"
# events themselves will be zstd compressed
psql -ddaq -c "CREATE UNLOGGED TABLE event_display (readout_number bigint PRIMARY KEY, run_number bigint NOT NULL, time timestamp with time zone NOT NULL DEFAULT now(), data bytea NOT NULL, type integer NOT NULL);"

echo "creating autoincrement function for event_display readout_number"
psql -ddaq -c 'CREATE OR REPLACE FUNCTION "fn_event_display_readout_number"() returns "pg_catalog"."trigger" as $BODY$ begin new.readout_number = (select COALESCE(MAX(readout_number),0)+1 from event_display); return NEW; end; $BODY$ LANGUAGE plpgsql VOLATILE COST 100;'
psql -ddaq -c 'CREATE TRIGGER trig_event_display_readout_number BEFORE insert ON event_display FOR EACH ROW EXECUTE PROCEDURE fn_event_display_readout_number();'

echo "creating index on event type"
psql -ddaq -c "CREATE INDEX ON event_display (readout_number) ${FAST_TABLESPACE};" # looking up a specific event
#psql -ddaq -c "CREATE INDEX ON event_display (run_number) ${FAST_TABLESPACE};"
psql -ddaq -c "CREATE INDEX ON event_display (type) ${FAST_TABLESPACE};" # looking up specific event types

echo "creating command_log table"
psql -ddaq -c "CREATE TABLE command_log (time timestamp with time zone NOT NULL DEFAULT now(), user_id integer references users(user_id), command json NOT NULL) PARTITION BY RANGE (time);"

# is this overkill with partitioning? maybe also change partitioning interval?
echo "creating index on command_log times"
psql -ddaq -c "CREATE INDEX ON command_log (time) ${FAST_TABLESPACE};"

echo "creating command_log template table"
psql -ddaq -c "CREATE TABLE command_log_template(LIKE command_log);"

echo "creating command_log partition parent and child tables"
psql -ddaq -c "SELECT partman.create_partition( p_parent_table:= 'public.command_log', p_control := 'time', p_interval := '1 day', p_template_table:='public.command_log_template');"

echo "creating locations table"
psql -ddaq -c "CREATE type tank_location as enum ('bottom', 'barrel', 'top');"
psql -ddaq -c "CREATE TABLE locations (id int PRIMARY KEY, x real NOT NULL, y real NOT NULL, z real, type text NOT NULL, size real NOT NULL, location tank_location NOT NULL);"

echo "creating shift_check table"
psql -ddaq -c "CREATE TABLE shift_check ( time timestamp with time zone NOT NULL DEFAULT now(), user_id integer references users(user_id), data jsonb NOT NULL) PARTITION BY RANGE (time);"

echo "creating index on shift check times"
psql -ddaq -c "CREATE INDEX ON shift_check (time) ${FAST_TABLESPACE};"

echo "creating shift_check template table"
psql -ddaq -c "CREATE TABLE shift_check_template(LIKE shift_check);"

echo "creating shift_check partition parent and child tables"
psql -ddaq -c "SELECT partman.create_partition( p_parent_table:= 'public.shift_check', p_control := 'time', p_interval := '1 day', p_template_table:='public.shift_check_template');"

echo "creating function to validate usernames"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.CheckUserExists(resolution_username text, expert_username text) RETURNS TABLE (resolution_ok boolean, expert_ok boolean) LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public' AS \$function$ SELECT EXISTS (SELECT 1 FROM public.users WHERE username = resolution_username) AS resolution_ok, EXISTS (SELECT 1 FROM public.users WHERE username = expert_username) AS expert_ok; \$function$"

echo "creating function to validate username/password"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.ValidateUser(p_username text, p_password_hash text) RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path TO 'public' AS \$function$ SELECT COALESCE((SELECT u.password_hash = p_password_hash FROM public.users u WHERE u.username = p_username), FALSE ); \$function$"

echo "Create UseridFromUsername function"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.UserIdFromUsername(p_username TEXT) RETURNS INTEGER LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public' AS \$function$ SELECT user_id FROM public.users WHERE username = p_username;\$function$"

echo "Create UsernameFromUserId function"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.UsernameFromUserId(p_user_id INTEGER ) RETURNS TEXT LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public' AS \$function$ SELECT username FROM public.users WHERE user_id = p_user_id;\$function$"

echo "Create RetireAllBaseConfigurations function"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.RetireAllBaseConfigurations() RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER SET search_path TO 'public' AS \$function$ UPDATE base_config SET retired = TRUE; SELECT TRUE \$function$"

echo "Create RetireDeviceConfigurationCascading function"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.RetireDeviceConfigurationCascading(p_device_name TEXT, p_version_num INTEGER) RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER SET search_path TO 'public' AS \$function$ \
UPDATE base_config SET retired = TRUE WHERE data ? p_device_name AND (data->p_device_name)::integer=p_version_num; \
UPDATE runmode_config SET retired = TRUE WHERE data ? p_device_name AND (data->p_device_name)::integer=p_version_num; \
UPDATE device_config SET retired = TRUE WHERE device=p_device_name AND version=p_version_num; \
SELECT TRUE \
\$function$"

echo "Create RetireManyDeviceConfigurationsCascading function"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.RetireManyDeviceConfigurationsCascading(p_device_name TEXT, p_version_num INTEGER) RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER SET search_path TO 'public' AS \$function$ \
UPDATE base_config SET retired = TRUE WHERE data ? p_device_name AND (data->p_device_name)::integer<p_version_num; \
UPDATE runmode_config SET retired = TRUE WHERE data ? p_device_name AND (data->p_device_name)::integer<p_version_num; \
UPDATE device_config SET retired = TRUE WHERE device=p_device_name AND version<p_version_num; \
SELECT TRUE \
\$function$"

echo "Create SilenceAlarm function"
psql -ddaq -c " CREATE OR REPLACE FUNCTION public.SilenceAlarm( p_uid INTEGER, p_user TEXT ) RETURNS TABLE(uid INTEGER, status INTEGER, silence_user TEXT) LANGUAGE sql SECURITY DEFINER SET search_path TO 'public' AS \$function$
    UPDATE public.alarms AS a SET status = 1, silence_user_id = public.UseridFromUsername(p_user) WHERE a.uid = p_uid RETURNING a.uid, a.status, a.silence_user;
\$function$"

echo "Create ResolveAlarm function"
psql -ddaq -c "CREATE OR REPLACE FUNCTION public.ResolveAlarm(
    p_uid INTEGER,
    p_resolution_user TEXT,
    p_expert_user TEXT,
    p_resolution_description TEXT
)
RETURNS TABLE(uid INTEGER, status INTEGER)
LANGUAGE sql SECURITY DEFINER SET search_path TO 'public'
AS \$function$
    UPDATE public.alarms AS a
    SET status = 2,
        resolve_user_id = public.UseridFromUsername(p_resolution_user),
        resolve_time = NOW(),
        resolution_description = p_resolution_description,
        expert_user_id = public.UseridFromUsername(p_expert_user)
    WHERE a.uid = p_uid
    RETURNING a.uid, a.status;
\$function$"

# add a database role for the webserver
echo "adding webserver database role"
psql -ddaq -c "CREATE ROLE webserver LOGIN"
psql -ddaq -c "GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO webserver;"
psql -ddaq -c "GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO webserver;"
psql -ddaq -c "GRANT EXECUTE ON ALL ROUTINES IN SCHEMA public TO webserver;"
psql -ddaq -c "GRANT CONNECT, TEMPORARY ON DATABASE daq TO webserver;"
psql -ddaq -c "REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM webserver;"
psql -ddaq -c "REVOKE CREATE ON SCHEMA public FROM webserver;"
psql -ddaq -c "REVOKE CREATE ON DATABASE daq FROM webserver;"
psql -ddaq -c "REVOKE SELECT ON TABLE users FROM webserver;"
# not sure if these are all needed, aren't they covered by usage of all routines on public?
psql -ddaq -c "GRANT EXECUTE ON FUNCTION ValidateUser(text, text) TO webserver";
psql -ddaq -c "GRANT EXECUTE ON FUNCTION public.CheckUserExists(text, text) TO webserver";
psql -ddaq -c 'GRANT EXECUTE ON FUNCTION public.UsernameFromUserId(integer) TO webserver;'
psql -ddaq -c "GRANT EXECUTE ON FUNCTION public.UserIdFromUsername(text) TO webserver;"
psql -ddaq -c "GRANT EXECUTE ON FUNCTION public.RetireAllBaseConfigurations() TO webserver;"
psql -ddaq -c "GRANT EXECUTE ON FUNCTION public.ResolveAlarm() TO webserver;"
psql -ddaq -c "GRANT EXECUTE ON FUNCTION public.SilenceAlarm() TO webserver;"

# add a database role for the middleman
echo "adding middleman role"
psql -ddaq -c "CREATE ROLE middleman LOGIN"
psql -ddaq -c "GRANT SELECT, INSERT, MAINTAIN ON ALL TABLES IN SCHEMA public TO middleman;"
psql -ddaq -c "REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM middleman;"
psql -ddaq -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO middleman;"
psql -ddaq -c "GRANT CONNECT, TEMPORARY ON DATABASE daq TO middleman;"
psql -ddaq -c "GRANT EXECUTE ON ALL ROUTINES IN SCHEMA public TO middleman;"
#psql -ddaq -c "GRANT SET ON PARAMETER xxx TO middleman;" allow changing some settings
psql -ddaq -c "REVOKE CREATE ON SCHEMA public FROM middleman;"
psql -ddaq -c "REVOKE CREATE ON DATABASE daq FROM middleman;"
# transaction timeout limits so people can't lock the middleman
psql -ddaq -c "ALTER ROLE middleman SET statement_timeout = 1000;"
psql -ddaq -c "ALTER ROLE middleman SET idle_in_transaction_timeout = 5000;"
psql -ddaq -c "ALTER ROLE middleman SET transaction_timeout = 10000;"
psql -ddaq -c "ALTER ROLE middleman SET lock_timeout = 10000;"
# may be overriden with `psql -c "set statement_timeout = '30s';"` within a given session

touch /.DBSetupDone

# run scripts to pre-populate
find ./scripts -type f -name '*.sh' -exec sh -c "if [ ! -f {}.done ]; then {} && touch {}.done; fi" \;

