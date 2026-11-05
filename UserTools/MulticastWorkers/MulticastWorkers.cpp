#include "MulticastWorkers.h"

MulticastWorkers::MulticastWorkers():Tool(){}

bool MulticastWorkers::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	logger = m_data->logger;
	//m_variables.Print();
	
//	// allocate ehhh 60% of the CPU to multicast workers
//	int max_workers= (double(std::thread::hardware_concurrency())*0.6);
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
//	m_variables.Get("max_workers",max_workers);
	
	ExportConfiguration();
	
	// potentially we will have a dedicated worker pool for multicast, but for now,
	// just one created and managed by JobManager Tool
	//job_manager = new WorkerPoolManager(multicast_jobs, &max_workers, 0, 0, 0, true, true);
	
	// monitoring struct to encapsulate tracking info
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.m_data = m_data;
	thread_args.monitoring_vars = &monitoring_vars;
	// thread needs a unique name
	if(!m_data->utils.CreateThread("multicast_job_distributor", &Thread, &thread_args)){
		LOG(logger,LOG_ERR,"Failed to spawn %s background thread",m_tool_name.c_str());
		return false;
	}
	m_data->num_threads++;
	
	last_exec = std::chrono::steady_clock::now();
	
	return true;
}

bool MulticastWorkers::Execute(){
	
	// FIXME ok but actually this kills all our jobs, not just our job distributor
	// so we don't want to do that.
	if(!thread_args.running){
		LOG(logger,LOG_ERR,"%s Execute found thread not running!",m_tool_name.c_str());
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	auto time_now = std::chrono::steady_clock::now();
	auto time_since_last = time_now - last_exec;
	if(time_since_last < std::chrono::milliseconds(1000)) return true;
	last_exec = time_now;
	
	/*printf("messages processed: %d (%.0f MB),\t logs: %d (%.0f MB),\tmons: %d (%.0f MB)\n",
	       monitoring_vars.msgs_processed.load(),
	       double(monitoring_vars.bytes_processed.load())/1E6,
	       monitoring_vars.logs_processed.load(),
	       double(monitoring_vars.logging_bytes_processed.load())/1E6,
	       monitoring_vars.mons_processed.load(),
	       double(monitoring_vars.monitoring_bytes_processed.load())/1E6);
	*/
	
	return true;
}

bool MulticastWorkers::Finalise(){
	
	// signal job distributor thread to stop
	LOG(logger,LOG_NOTICE,"%s Joining receiver thread",m_tool_name.c_str());
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	// this will invoke kill on the WorkerPoolManager thread creating worker threads, as well as all workers.
	//delete job_manager;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	LOG(logger,LOG_NOTICE,"%s Finished",m_tool_name.c_str());
	return true;
}


void MulticastWorkers::Thread(Thread_args* args){
	
	MulticastJobDistributor_args* m_args = dynamic_cast<MulticastJobDistributor_args*>(args);
	m_args->local_msg_queue.clear();
	
	// grab any batches of logging/monitoring messages
	std::unique_lock<std::mutex> locker(m_args->m_data->in_multicast_msg_queue_mtx);
	if(!m_args->m_data->in_multicast_msg_queue.empty()){
		std::swap(m_args->m_data->in_multicast_msg_queue, m_args->local_msg_queue);
	} else {
		locker.unlock();
		usleep(100);
		return;
	}
	locker.unlock();
	
	// add a job for each batch to the queue
	for(int i=0; i<m_args->local_msg_queue.size(); ++i){
		
		// add a new Job to the job queue to process this data
		Job* the_job = m_args->m_data->job_pool.GetNew("multicast_worker");
		the_job->out_pool = &m_args->m_data->job_pool;
		if(the_job->data == nullptr){
			// on first creation of the job, make it a JobStruct to encapsulate its data
			// N.B. Pool::GetNew will only invoke the constructor if this is a new instance,
			// (not if it's been used before and then returned to the pool)
			// so don't pass job-specific variables to the constructor
			the_job->data = m_args->job_struct_pool.GetNew(&m_args->job_struct_pool, m_args->m_data, m_args->monitoring_vars);
		} else {
			// this should never happen as jobs should return their args to the pool
			LOG(m_args->m_data->logger,LOG_ERR,"Multicast Job with non-null data pointer!");
			// FIXME ... do we assume this job args object is valid, and use it?
			// this could lead to a segfault (if the args got returned to the pool and deleted)
			// or corruption (if the args got returned to the pool and given to another job)
			// alternatively do we just over-write the job pointer with new args (potentially leaking it)
		}
		MulticastJobStruct* job_data = static_cast<MulticastJobStruct*>(the_job->data);
		job_data->monitoring_vars = m_args->monitoring_vars;
		job_data->m_job_name = "multicast_worker";
		job_data->msg_buffer = m_args->local_msg_queue[i];
		job_data->logging_buffer = m_args->m_data->multicast_batch_pool.GetNew();
		job_data->monitoring_buffer = m_args->m_data->multicast_batch_pool.GetNew();
		job_data->rootplot_buffer = m_args->m_data->multicast_batch_pool.GetNew();
		job_data->plotlyplot_buffer = m_args->m_data->multicast_batch_pool.GetNew();
		
		the_job->func = MulticastMessageJob;
		the_job->fail_func = MulticastMessageFail;
		
		//multicast_jobs.AddJob(the_job);
		//printf("spawning new multicastjob for %d messages\n",job_data->msg_buffer->size());
		m_args->m_data->job_queue.AddJob(the_job);
		//++(m_args->monitoring_vars.jobs_submitted);
		
	}
	
	return;
}


// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void MulticastWorkers::MulticastMessageFail(void*& arg){
	
	// safety check in case the job somehow fails after returning its args to the pool
	if(arg==nullptr){
		std::cerr<<"multicast worker fail with no args"<<std::endl;
		return;
	}
	
	MulticastJobStruct* m_args=static_cast<MulticastJobStruct*>(arg);
	LOG(m_args->m_data->logger,LOG_ERR,"%s job failure",m_args->m_job_name);
	++(m_args->monitoring_vars->jobs_failed);
	
	// return the vector of string buffers to the pool for re-use by MulticastReceiverSender Tool
	m_args->msg_buffer->clear();
	m_args->m_data->multicast_buffer_pool.Add(m_args->msg_buffer);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	// FIXME do something here
	// we could also try to insert the buffers into the queues for downstream,
	// if there were preceding messages that were succesfully added.
	// but we don't know where we failed, so that could be risky.
	// we could keep track of where we were in m_args and:
	// 1. log the specific message we were trying to process when the job failed
	// 2. submit the data we already have
	// 3. make a new job for the remaining data
	
	return;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

// Each job takes a vector of messages and converts them into a suitable object,
// then locks and inserts that into a datamodel vector for the database workers
bool MulticastWorkers::MulticastMessageJob(void*& arg){
	
	MulticastJobStruct* m_args=static_cast<MulticastJobStruct*>(arg);
	
	thread_local std::unique_ptr<ZSTD_DCtx,long unsigned int(*)(ZSTD_DCtx*)> zstd_ctx(ZSTD_createDCtx(), ZSTD_freeDCtx);
	
	// most efficient way to do insertion would seem to be via json_to_recordset, which allows batching queries,
	// query optimisation similar to 'unnest', and avoids the overhead of parsing the JSON: e.g.
	// psql -c "INSERT INTO logging ( time, device, severity, message ) SELECT * FROM 
	// json_to_recordset('[ {\"time\":\"2025-12-01 12:31\", \"device\":\"dev1\", \"severity\":1, \"message\":\"blah\"},
	//                       {\"time\":\"2025-12-02 15:25\", \"device\":\"dev2\", \"severity\":2, \"message\":\"arg\"} ]')
	// as t(time timestamptz, device text, severity int, message text);"  << (this part is needed)
	
	// or:
	// PREPARE loginsert ( text ) AS INSERT INTO logging ( time, device, severity, message ) SELECT * FROM json_to_recordset( $1::json ) as t(time timestamptz, device text, severity int, message text);
	// then:
	// execute loginsert('[ {"time":"2025-12-01 12:31", "device":"dev1", "severity":1, "message":"blah"}, {"time":"2025-12-02 15:25", "device":"dev2", "severity":2, "message":"oooh"} ]');
	
	// subsequently, all we need to do here is concatenate the JSONs
	
	//printf("%s processing %d batches\n",m_args->m_job_name.c_str(), m_args->msg_buffer->size());
	
	*m_args->logging_buffer = "[";
	*m_args->monitoring_buffer = "[";
	*m_args->rootplot_buffer = "[";
	*m_args->plotlyplot_buffer = "[";
	
	m_args->n_log_msgs = 0;
	m_args->n_mon_msgs = 0;
	
	// loop over messages
	for(std::string& next_msg : *m_args->msg_buffer){
		
		//printf("next monitoring msg: '%s'\n",next_msg.c_str());
		// message may be compressed or decompressed, as indicated by first bye
		if(ZmqQuery::compressed(next_msg)){
			// compressed - decompress it
			m_args->decompressed_bytes = ZSTD_getFrameContentSize(next_msg.data(), next_msg.size());
			if(m_args->decompressed_bytes==ZSTD_CONTENTSIZE_UNKNOWN || m_args->decompressed_bytes==ZSTD_CONTENTSIZE_ERROR){
				// bad message, discard
				LOG(m_args->m_data->logger,LOG_WARNING,"%s ignoring zstd bad multicast message '%s'\n",
				    m_args->m_job_name.c_str(), ZSTD_getErrorName(m_args->decompressed_bytes)/*next_msg.c_str()*/);
				continue;
			}
			if(m_args->decompressed_bytes > MAX_DECOMPRESSED_MSG_SIZE){
				LOG(m_args->m_data->logger,LOG_WARNING,"%s ignoring zstd message requesting excessive '%lu' byte decompression buffer\n",m_args->m_job_name.c_str(), m_args->decompressed_bytes);
				continue;
			}
			m_args->decompress_buffer.resize(m_args->decompressed_bytes);
			m_args->decompressed_bytes = ZSTD_decompressDCtx(zstd_ctx.get(),(void*)m_args->decompress_buffer.data(),m_args->decompressed_bytes, next_msg.data(), next_msg.size());
			if(ZSTD_isError(m_args->decompressed_bytes)){
				LOG(m_args->m_data->logger,LOG_WARNING,"%s error decompressing zstd message: %s\n",
				    m_args->m_job_name.c_str(), ZSTD_getErrorName(m_args->decompressed_bytes));
				continue;
			}
			m_args->the_msg = std::string_view(m_args->decompress_buffer.c_str(),m_args->decompressed_bytes);
		} else {
			m_args->the_msg = std::string_view(next_msg.c_str(),next_msg.size());
		}
		
		// we can't batch insertions destined for different tables,
		// so keep each message type (topic) in a different buffer.
		// the Services class always puts the topic first,
		// and all topics start with a unique character (XXX for now?),
		// so we don't need to parse the message to identify the topic:
//		printf("validating first 9 chars are topic: '%s', %d\n",m_args->the_msg.substr(0,9).c_str(),strcmp(m_args->the_msg.substr(0,9).c_str(),"{\"topic\":"));
		if(m_args->the_msg.substr(0,9)!="{\"topic\":"){
			LOG(m_args->m_data->logger,LOG_WARNING,"%s ignoring bad multicast message '%.*s'\n",
			    m_args->m_job_name.c_str(), m_args->the_msg.size(), m_args->the_msg.data());
			continue;
		}
		
		switch(query_topic{(m_args->the_msg)[10]}){
			case query_topic::logging:
				m_args->out_buffer = m_args->logging_buffer;
				++m_args->n_log_msgs;
				break;
			case query_topic::monitoring:
				m_args->out_buffer = m_args->monitoring_buffer;
				++m_args->n_mon_msgs;
				break;
			case query_topic::rootplot:
				m_args->out_buffer = m_args->rootplot_buffer;
				break;
			case query_topic::plotlyplot:
				m_args->out_buffer = m_args->plotlyplot_buffer;
				break;
			default:
				LOG(m_args->m_data->logger,LOG_WARNING,"%s unknown multicast topic '%c' in message '%.*s'\n",
				    m_args->m_job_name.c_str(), (m_args->the_msg)[10],m_args->the_msg.size(), m_args->the_msg.data());
				continue;
		}
		
		// FIXME can we make this use moving write-head instead of copying?
		if(m_args->out_buffer->length()>1) (*m_args->out_buffer) += ",";
		(*m_args->out_buffer) += m_args->the_msg;
		//printf("%s added message '%.*s'\n",m_args->m_job_name.c_str(), m_args->the_msg.length(), m_args->the_msg.data());
		
		m_args->monitoring_vars->bytes_processed += m_args->the_msg.size(); // FIXME assumes this job completes successfully
		
	}
	
	// pass into datamodel for DatabaseWorkers
	if(m_args->logging_buffer->length()!=1){
		*m_args->logging_buffer += "]";
		std::unique_lock<std::mutex> locker(m_args->m_data->log_query_queue_mtx);
		m_args->m_data->log_query_queue.push_back(m_args->logging_buffer);
		//printf("%s adding '%s' to logging buffer\n",m_args->m_job_name.c_str(), m_args->logging_buffer->c_str());
	}
	
	if(m_args->monitoring_buffer->length()!=1){
		*m_args->monitoring_buffer += "]";
		//printf("pushing batch message: '%s'\n",m_args->monitoring_buffer->c_str());
		std::unique_lock<std::mutex> locker(m_args->m_data->mon_query_queue_mtx);
		m_args->m_data->mon_query_queue.push_back(m_args->monitoring_buffer);
	}
	
	if(m_args->rootplot_buffer->length()!=1){
		*m_args->rootplot_buffer += "]";
		std::unique_lock<std::mutex> locker(m_args->m_data->rootplot_query_queue_mtx);
		m_args->m_data->rootplot_query_queue.push_back(m_args->rootplot_buffer);
	}
	
	if(m_args->plotlyplot_buffer->length()!=1){
		*m_args->plotlyplot_buffer += "]";
		std::unique_lock<std::mutex> locker(m_args->m_data->plotlyplot_query_queue_mtx);
		m_args->m_data->plotlyplot_query_queue.push_back(m_args->plotlyplot_buffer);
	}
	
	// return the vector of string buffers to the pool for re-use by MulticastReceiverSender Tool
	m_args->msg_buffer->clear();
	m_args->m_data->multicast_buffer_pool.Add(m_args->msg_buffer);
	
	//printf("%s job completed\n",m_args->m_job_name.c_str());
	++(m_args->monitoring_vars->jobs_completed);
	m_args->monitoring_vars->msgs_processed += m_args->msg_buffer->size();
	m_args->monitoring_vars->logs_processed += m_args->n_log_msgs;
	m_args->monitoring_vars->mons_processed += m_args->n_mon_msgs;
	if(m_args->n_log_msgs!=0) m_args->monitoring_vars->logging_bytes_processed += m_args->logging_buffer->length() - 1 - m_args->n_log_msgs; // subtractions account for [ , ] i think
	if(m_args->n_mon_msgs!=0) m_args->monitoring_vars->monitoring_bytes_processed += m_args->monitoring_buffer->length() - 1 - m_args->n_mon_msgs;
	
	
	m_args->m_pool->Add(m_args);  // return our job args to the job args struct pool
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return true;
	
}
















































/*
//	// used with v0 batching
//	static const std::string log_base = "INSERT INTO logging ( time, device, severity, message ) VALUES ";
//	static const std::string mon_base = "INSERT INTO monitoring ( time, device, subject, data ) VALUES ";

// Each job takes a vector of messages and converts them into a suitable object,
// then locks and inserts that into a datamodel vector for the database workers
void MulticastWorkers::MulticastMessageJob(void* arg){
	
	//=================
	// v0: combine all multicasts into a batch sql query (curent version)
	// note! supposedly cannot use this with pqxx::pipeline (see 'pqxx::pipeline::insert')
	// although if we're not interested in any return values, maybe it's ok...?
	
	// v1: insert into pipeline - does batching for you, so maybe equivalent to v0?
	// accepts a std::stringview of a query, so still need to do sanitization yourself,
	// and be mindful of lifetime of the query you pass it!
	
	// v2: turn each multicast into a form suitable for use with pqxx::stream
	// the fastest method is pqxx::stream::write_values(T...) which accepts a set of variables
	// less preferred is write_row or operator<< both of which accept a container or tuple
	
	// v3: turn each multicast message into a pqxx::params object to be used with a prepared statement (pqxx::prepped)
	
	// v4: transpose the data and use unnest to pass multiple rows as a set of columns
	// this should be close in performance to COPY (stream)
	// psql -c "INSERT INTO logging ( time, device, severity, message ) SELECT * FROM 
	// UNNEST(ARRAY['2025-12-01 12:31', '2025-12-02 15:23']::timestamptz[], 
	//        ARRAY['dev1', 'dev2'],
	//       '{1,2}'::int[],                      << alternative way to define an array of ints (note bracket change)
	//       '{\"blah\", \"argh\"}'::text[])"     << for array of strings need internal quoting
	
	// v5: just insert the JSON directly 5-head
	// psql -c "INSERT INTO logging ( time, device, severity, message ) SELECT * FROM 
	// json_to_recordset('[ {\"time\":\"2025-12-01 12:31\", \"device\":\"dev1\", \"severity\":1, \"message\":\"blah\"},
	//                       {\"time\":\"2025-12-02 15:25\", \"device\":\"dev2\", \"severity\":2, \"message\":\"arg\"} ]')
	// as t(time timestamptz, device text, severity int, message text);"  << this part is needed
	
	PREPARE moninsert ( text ) as INSERT INTO monitoring ( time, device, subject, data ) select * from json_to_recordset( $1::json ) as t(time timestamptz, device text, subject text, data json );
	execute moninsert('[ {"time":"2025-12-03 12:22", "device":"dev3", "subject":"test", "data":{"testkey":"testval", "key2":3} }, {"time":"2025-12-03 13:23", "device":"dev3", "subject":"test", "data":{"testkey":"testval2", "key2":4} } ]' );
	
	//==================
	
	MulticastJobStruct* m_args=static_cast<MulticastJobStruct*>(arg);
	
	// v0: pre-populate query with base
	m_args->out_buffer = m_args->query_base;
	
	// loop over messages, parse each into an SQL query
	for(std::string& next_msg : *m_args->msg_buffer){
		
//		// parse message
//		thread_local MulticastMsg msg;
//		msg.Clear();
//		if(!msg.Parse(message)){
//			Log("MulticastMessageToQuery error parsing message json '"+message+"'",v_error); // FIXME track, report
//			continue;
//		}
		
		// we can't batch insert of records into different tables
		// so keep each message type (topic) in a different buffer
//		if(msg.topic=="logging"){
//			m_args->out_buffer = m_args->logging_buffer;
//		} else if(topic=="monitoring"){
//			m_args->out_buffer = m_args->monitoring_buffer;
//		} else if(topic=="rootplot"){
//			m_args->out_buffer = m_args->rootplot_buffer;
//		} else if(msg.topic=="plotlyplot"){
//			m_args->out_buffer = m_args->plotlyplot_buffer;
//		}
		
//		// v0: concatenate to batch query
//		m_args->out_buffer += msg.GetString(m_args->first_vals); // FIXME: sanitization
		
//		// v1: insert into pipeline
//		m_args->out_buffer.insert(msg.GetString()); // FIXME lifetime of this string needs to persist.... how to do?
		
//		// v2: append to queue of tuples for stream   // n.b. need to split into GetMonitoringTuple/GetLoggingTuple
//		m_args->out_buffer->push_back(msg.GetTuple()); // because the tuple types are different
//		or
//		m_args->out_buffer->push_back(msg);            // what's the deal here?
//		// well the preferred way to use a stream is pqxx::stream_to(a, b c) for variables a,b,c
//		// we implement this via Msg::StreamRow(pqxx::stream_to), but this Tool doesn't have the pqxx::stream_to
//		
//		// v3: append to queue of pqxx::params objects for prepared statement
//		m_args->out_buffer->push_back(msg.GetParams());
		
//		// v4: unnest
//		m_args->out_buffer.Append(msg);
//		// it's going to be some kind of struct that internally has strings for a list of timestamps,
//		// device names, severities and messages. Append adds this messages' new values to each.
		
		++m_args->n_queries; // FIXME make atomic, stats tracking
		
	}
	
//	// v0: terminate this batch with semicolon
//	m_args->out_buffer += ";";
	
	...
	
}
*/
