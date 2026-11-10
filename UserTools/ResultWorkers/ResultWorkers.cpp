#include "ResultWorkers.h"

ResultWorkers::ResultWorkers():Tool(){}

// FIXME sufficient?
const size_t COMPRESS_BUFFER_SIZE=633565;
bool ResultWorkers::msg_compression=true;
int ResultWorkers::compression_level=0;

bool ResultWorkers::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	logger = m_data->logger;
	//m_variables.Print();
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	if(!m_variables.Get("msg_compression",msg_compression)) msg_compression=true;
	if(!m_variables.Get("compression_level",compression_level)) compression_level=1;
	
	ExportConfiguration();
	
	// monitoring struct to encapsulate tracking info
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.m_data = m_data;
	thread_args.monitoring_vars = &monitoring_vars;
	if(!m_data->utils.CreateThread("result_job_distributor", &Thread, &thread_args)){
		LOG(logger,LOG_ERR,"Failed to spawn %s background thread",m_tool_name.c_str());
		return false;
	}
	m_data->num_threads++;
	
	return true;
}


bool ResultWorkers::Execute(){
	
	// FIXME ok but actually this kills all our jobs, not just our job distributor
	// so we don't want to do that.
	if(!thread_args.running){
		LOG(logger,LOG_ERR,"%s Execute found thread not running!",m_tool_name.c_str());
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	return true;
}


bool ResultWorkers::Finalise(){
	
	// signal job distributor thread to stop
	LOG(logger,LOG_NOTICE,"%s Joining receiver thread",m_tool_name.c_str());
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	LOG(logger,LOG_NOTICE,"%s Finished",m_tool_name.c_str());
	return true;
}


void ResultWorkers::Thread(Thread_args* args){
	
	ResultJobDistributor_args* m_args = reinterpret_cast<ResultJobDistributor_args*>(args);
	
	// grab a batch of read queries, with results awaiting conversion
	std::unique_lock<std::mutex> locker(m_args->m_data->query_results_mtx);
	if(m_args->m_data->query_results.empty()){
		locker.unlock();
		usleep(100);
		return;
	}
	std::swap(m_args->m_data->query_results, m_args->local_msg_queue);
	locker.unlock();
	
	// add a job for each batch to the queue
	for(int i=0; i<m_args->local_msg_queue.size(); ++i){
		
		// add a new Job to the job queue to process this data
		Job* the_job = m_args->m_data->job_pool.GetNew("result_worker");
		the_job->out_pool = &m_args->m_data->job_pool;
		if(the_job->data == nullptr){
			// on first creation of the job, make it a JobStruct to encapsulate its data
			// N.B. Pool::GetNew will only invoke the constructor if this is a new instance,
			// (not if it's been used before and then returned to the pool)
			// so don't pass job-specific variables to the constructor
			the_job->data = m_args->job_struct_pool.GetNew(&m_args->job_struct_pool, m_args->m_data, m_args->monitoring_vars);
		} else {
			LOG(m_args->m_data->logger,LOG_ERR,"result_worker Job with non-null data pointer!");
		}
		
		the_job->func = ResultJob;
		the_job->fail_func = ResultJobFail;
		
		ResultJobStruct* job_data = static_cast<ResultJobStruct*>(the_job->data);
		job_data->batch = m_args->local_msg_queue[i];
		job_data->m_job_name = "result_worker";
		
		//job_data->batch->push_time("result_job_push");
		
		m_args->m_data->job_queue.AddJob(the_job);
		
	}
	m_args->local_msg_queue.clear();
	
	return;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void ResultWorkers::ResultJobFail(void*& arg){
	
	// safety check in case the job somehow fails after returning its args to the pool
	if(arg==nullptr){
		SLOG(LOG_ERR,"multicast worker fail with no args");
		return;
	}
	
	// FIXME hmm, well, i guess we say the query failed
	// - we had the results, but then lost them before sending
	
	ResultJobStruct* m_args=reinterpret_cast<ResultJobStruct*>(arg);
	LOG(m_args->m_data->logger,LOG_ERR,"%s job failure",m_args->m_job_name.c_str());
	++(m_args->monitoring_vars->jobs_failed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return;
}

bool ResultWorkers::ResultJob(void*& arg){
	
	ResultJobStruct* m_args = reinterpret_cast<ResultJobStruct*>(arg);
	
	thread_local ZSTD_CCtx* zstd_ctx = ZSTD_createCCtx();
	thread_local char* compress_buf = new char[ZSTD_compressBound(COMPRESS_BUFFER_SIZE)];
	
	//m_args->batch->push_time("result_worker_start");
	
	// for now each job processes a batch, not a set of batches
	//for(QueryBatch* batch : m_args->local_msg_queue){
		
		// read queries need to have their results interpreted,
		// write queries only need to have their insertion success status returned.
		// each batch should only contain messages that are either all read or all write.
		if(m_args->batch->queries.front().topic()[0]=='R'){
			
			// process batch of read queries
			
			for(ZmqQuery& query : m_args->batch->queries){
				
				// one topic is not yet set - retreivals from config cache
				// handle those now
				if(query_topic{query.topic()[2]}==query_topic::cached_config){
					std::string query_string{query.msg()}; // can't use string_view as a key to a map lookup :T
					if(m_args->m_data->cached_configs.count(query_string)){
						query.setsuccess(1);
						query.setresponserows(1);
						query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->m_data->cached_configs[query_string]));
					} else {
						query.setsuccess(0);
						query.setresponserows(1);
						query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, "no cached configuration for this device!"));
					}
				}
				
				// set whether the query succeeded or threw an exception
				else if(!query.err.empty()){
					query.setsuccess(0);
					query.setresponserows(1);
					query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, query.err));
					
				} else {
					query.setsuccess(1);
					
					// returned rows are sent back formatted as JSON, with each row a new zmq::message_t
					// resize zmq vector in preparation
					query.setresponserows(std::size(query.result));
					// should always be 0 or 1 rows FIXME we could add a check for that here
					
					// just for good measure, when we try to access the pqxx result,
					// enclose within try just in case it throws something
					try {
						// standard queries generated by the libDAQInterface use `row_to_json`
						// to request results already packaged up into one JSON per row
						// so all we need to do is copy that into the zmq message
						for(size_t i=0; i<std::size(query.result); ++i){
							query.setresponse(i, CompressMsg(zstd_ctx, compress_buf, query.result[i][0].c_str()));
						}
					} catch (std::exception& e){
						LOG(m_args->m_data->logger,LOG_ERR,"caught %s trying to access query result!",e.what());
						query.setsuccess(0);
						query.setresponserows(1);
						query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, "error accessing query result"));
						++(m_args->monitoring_vars->result_access_errors);
					}
					
					// release pqxx::result and clear error
					query.Clear();
					
				} // if we had a result object
			} // loop over queries in this batch
			
			++(m_args->monitoring_vars->read_batches_processed);
			
		} else {
			
			// process batch of write queries
			// these are interleaved but results are grouped by type
			size_t devconfig_i = 0;
			size_t base_config_i = 0;
			size_t runmode_config_i = 0;
			size_t calibration_i = 0;
			size_t plotlyplot_i = 0;
			size_t rootplot_i = 0;
			bool devconfigs_ok = !m_args->batch->devconfig_version_nums.empty();
			bool base_configs_ok = !m_args->batch->base_config_version_nums.empty();
			bool runmode_configs_ok = !m_args->batch->runmode_config_version_nums.empty();
			bool calibrations_ok = !m_args->batch->calibration_version_nums.empty();
			bool plotlyplots_ok = !m_args->batch->plotlyplot_version_nums.empty();
			bool rootplots_ok = !m_args->batch->rootplot_version_nums.empty();
			
			for(ZmqQuery& query : m_args->batch->queries){
				
				switch(query_topic{query.topic()[2]}){
					// alarms return just the success status
					case query_topic::alarm:
						query.setsuccess(m_args->batch->alarm_batch_err.empty());
						query.setresponserows(0);
						break;
						
					// everything else returns a version number
					case query_topic::dev_config:
						query.setsuccess(devconfigs_ok);
						query.setresponserows(1);
						if(devconfigs_ok){
							// don't compress numerics
							query.setresponse(0, m_args->batch->devconfig_version_nums[devconfig_i++]);
						} else {
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->batch->devconfig_batch_err));
						}
						break;
						
					case query_topic::base_config:
						query.setsuccess(base_configs_ok);
						query.setresponserows(1);
						if(base_configs_ok){
							query.setresponse(0, m_args->batch->base_config_version_nums[base_config_i++]);
						} else {
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->batch->base_config_batch_err));
						}
						break;
						
					case query_topic::runmode_config:
						query.setsuccess(runmode_configs_ok);
						query.setresponserows(1);
						if(runmode_configs_ok){
							query.setresponse(0, m_args->batch->runmode_config_version_nums[runmode_config_i++]);
						} else {
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->batch->runmode_config_batch_err));
						}
						break;
						
					case query_topic::calibration:
						query.setsuccess(calibrations_ok);
						query.setresponserows(1);
						if(calibrations_ok){
							query.setresponse(0, m_args->batch->calibration_version_nums[calibration_i++]);
						} else {
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->batch->calibration_batch_err));
						}
						break;
						
					case query_topic::plotlyplot:
						query.setsuccess(plotlyplots_ok);
						query.setresponserows(1);
						if(plotlyplots_ok){
							query.setresponse(0, m_args->batch->plotlyplot_version_nums[plotlyplot_i++]);
						} else {
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->batch->plotlyplot_batch_err));
						}
						break;
						
					case query_topic::rootplot:
						query.setsuccess(rootplots_ok);
						query.setresponserows(1);
						if(rootplots_ok){
							query.setresponse(0, m_args->batch->rootplot_version_nums[rootplot_i++]);
						} else {
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, m_args->batch->rootplot_batch_err));
						}
						break;
						
					case query_topic::generic:
						
						if(!query.err.empty()){
							query.setsuccess(0);
							query.setresponserows(1);
							query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, query.err));
							
						} else {
							query.setsuccess(1);
							
							try {
								// we need to loop over rows and convert them to JSON manually
								query.setresponserows(std::size(query.result));
								for(size_t i=0; i<std::size(query.result); ++i){
									
									// build a json from fields in this row
									m_args->tmpval = "{";
									for (pqxx::row::iterator it=query.result[i].begin(); it<query.result[i].end(); ++it){
										if(it!=query.result[i].begin()) m_args->tmpval += ", ";
										m_args->tmpval += "\"" + std::string{it->name()} + "\":";
										// Field values are returned bare: i.e. '3' or 'cat' or '{"iam":"ajson"}'
										// but to convert this into JSON, strings need to be quoted:
										// i.e. { "field1":3, "field2":"cat", "field3":{"iam":"ajson"} }
										// this means we need to add enclosing quotes *only* for string fields
										// 18 = char, 25 = text, 1043 = varchar, 1042 = bpchar
										// these are oids from psql -c "select * from pg_type" or somewhere
										if((it->type()==18) || (it->type()==25) || (it->type()==1042) || (it->type()==1043)){
											m_args->tmpval += "\""+std::string{it->c_str()}+"\"";
										} else {
											m_args->tmpval += it->c_str();
										}
									}
									m_args->tmpval += "}";
									
									query.setresponse(i, CompressMsg(zstd_ctx, compress_buf, m_args->tmpval));
								}
								
							} catch (std::exception& e){
								LOG(m_args->m_data->logger,LOG_ERR,"caught %s: %s trying to access query result!",
								    current_exception_name().c_str(),e.what());
								query.setsuccess(0);
								query.setresponserows(1);
								query.setresponse(0, CompressMsg(zstd_ctx, compress_buf, "Error accessing query result"));
								++(m_args->monitoring_vars->result_access_errors);
							}
						}
						
						break;
						
					default:
						// FIXME corrupted topic, log it.
						LOG(m_args->m_data->logger,LOG_ERR,"%s unknown topic %.*s",
						    m_args->m_job_name,query.topic().size(),query.topic().data());
						break;
					
				}
				
				// release pqxx::result and clear error
				query.Clear();
				
			} // loop over queries in this batch
			
			++(m_args->monitoring_vars->write_batches_processed);
			
		} // if/else on whether this batch was read/write
		
		//m_args->batch->push_time("result_done");
		
//	} // loop over query batches
	
	// pass the batch onto the next stage of the pipeline for the DatabaseWorkers
	std::unique_lock<std::mutex> locker(m_args->m_data->query_replies_mtx);
	//m_args->m_data->query_replies.insert(m_args->m_data->query_replies.end(),
	//                                     m_args->local_msg_queue.begin(),m_args->local_msg_queue.end());
	m_args->m_data->query_replies.push_back(m_args->batch);
	locker.unlock();
	
	//printf("%s completed\n",m_args->m_job_name.c_str());
	++(m_args->monitoring_vars->jobs_completed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);  // return our job args to the job args struct pool
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return true;
}

std::string_view ResultWorkers::CompressMsg(ZSTD_CCtx* zstd_ctx, char* compress_buf, const std::string& msg){
	if(zstd_ctx){
		thread_local size_t compressed_bytes;
		compressed_bytes = ZSTD_compressCCtx(zstd_ctx, compress_buf, COMPRESS_BUFFER_SIZE, msg.data(), msg.size(), compression_level);
		if(ZSTD_isError(compressed_bytes)){
			SLOG(LOG_ERR,"zstd error: %s\n", ZSTD_getErrorName(compressed_bytes));
			// we'll fall back to uncompressed
		} else {
			return std::string_view(compress_buf, compressed_bytes);
		}
	}
	return std::string_view(msg);
}
