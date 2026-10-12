#include "MulticastReceiverSender.h"

#include <chrono>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <set>

namespace {
  const uint32_t MAX_UDP_PACKET_SIZE = 655355;
}

MulticastReceiverSender::MulticastReceiverSender():Tool(){}


bool MulticastReceiverSender::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	logger=m_data->logger;
	//m_variables.Print();
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
	m_verbose=1;
	int port = 5000; // shared with service discovery, logging and monitoring
	std::string multicast_address; // separate for each
	// FIXME slow controls to vary them
	int local_buffer_size = 100;
	int transfer_period_ms = 1000;
	int poll_timeout_ms = 100;
	
	m_variables.Get("type",type_str);
	if(type_str!="logging" && type_str!="monitoring"){
		LOG(logger,LOG_ERR,"invalid port type '%s'; valid values are 'logging' and 'monitoring'",type_str.c_str());
		return false;
	}
	m_variables.Get("verbose",m_verbose);
	m_variables.Get("port",port);
	if(!m_variables.Get("multicast_address",multicast_address)){
		if(type_str=="logging") multicast_address = "239.192.1.2";
		else multicast_address = "239.192.1.3";
	}
	LOG(logger,LOG_NOTICE,"listening for %s traffic on %s:%d",type_str.c_str(), multicast_address.c_str(),port);
	
	std::string out_interface_address="";
	if(!m_variables.Get("multicast_out_address",out_interface_address)){
		LOG(logger,LOG_NOTICE,"%s output interface not specified, leaving to default",type_str.c_str());
	} else {
		LOG(logger,LOG_NOTICE,"outgoing %s will use interface %s",type_str.c_str(),out_interface_address.c_str());
	}
	
	// buffer received messages in a local vector until size exceeds local_buffer_size...
	m_variables.Get("local_buffer_size",local_buffer_size);
	// ... or time since last transfer exceeds transfer_period_ms
	m_variables.Get("transfer_period_ms",transfer_period_ms);
	m_variables.Get("poll_timeout_ms",poll_timeout_ms);
	
	ExportConfiguration();
	
	// init
	last_bytes_in=0;
	last_bytes_out=0;
	last_bytes_time=std::chrono::steady_clock::now();
	
	/* ----------------------------------------- */
	/*               Socket Setup                */
	/* ----------------------------------------- */
	
	socket_handle = socket(AF_INET, SOCK_DGRAM, 0);
	if(socket_handle<=0){
		LOG(logger,LOG_ERR,"Failed to open %s socket with error %s",type_str.c_str(),strerror(errno));
		return false;
	}
	
	// set buffer size to increase it. n.b. buffer size will be doubled, apprently.
	int rcvbuf = 128 * 1024 * 1024; // 128MB (but actually 256?)
	setsockopt(socket_handle, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
	
	// set linger options - do not linger, discard queued messages on socket close
	struct linger l;
	l.l_onoff  = 0;  // whether to linger
	l.l_linger = 0;  // seconds to linger for
	get_ok = setsockopt(socket_handle, SOL_SOCKET, SO_LINGER, (char*) &l, sizeof(l));
	if(get_ok!=0){
		LOG(logger,LOG_ERR,"Failed to set %s socket linger with error %s",type_str.c_str(),strerror(errno));
		return false;
	}
	
	// disable blocking connections to this ip+port fomr TIME_WAIT after closure.
	// this is intended to prevent delivery of delayed packets to the wrong application,
	// but means a new middleman instance won't be able to bind for 30-120 seconds after another closes.
	int a =1;
	get_ok = setsockopt(socket_handle, SOL_SOCKET, SO_REUSEADDR, &a, sizeof(a));
	if(get_ok!=0){
		LOG(logger,LOG_ERR,"Failed to set %s socket reuseaddr with error %s",type_str.c_str(),strerror(errno));
		return false;
	}
	
	// set the socket to non-blocking mode - should be irrelevant as we poll
	get_ok = fcntl(socket_handle, F_SETFL, O_NONBLOCK);
	if(get_ok!=0){
		LOG(logger,LOG_WARNING,"Failed to set %s socket to non-blocking with error %s",type_str.c_str(),strerror(errno));
	}
	
	
	// format destination address from IP string
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	bzero((char *)&addr, addrlen); // init to 0
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	
	// to receive traffic from a specific group, either bind to that group *and* join the group
//	inet_aton(multicast_address.c_str(), &addr.sin_addr);
	// or bind to INADDR_ANY, disable IP_MULTICAST_ALL, and then join the group
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	a=0;
	setsockopt(socket_handle, IPPROTO_IP, IP_MULTICAST_ALL, &a, sizeof(int));
	// some additional complexity: to receive multicast packets from that group on any interface, we should bind to INADDR_ANY
	// and then join that multicast group manually on each interface
	
	/* FIXME FIXME FIXME
	// sending: which multicast group to send to
	get_ok = inet_aton(multicast_address.c_str(), &addr.sin_addr);
	if(get_ok==0){ // returns 0 if invalid, unlike other functions
		Log("Bad multicast address '"+multicast_address+"'",v_error);
		return false;
	}
	
	// for two-way comms, we should bind to INADDR_ANY, not a specific multicast address.... maybe?
	struct sockaddr_in multicast_addr2;
	bzero((char *)&multicast_addr2, sizeof(multicast_addr2)); // init to 0
	multicast_addr2.sin_family = AF_INET;
	multicast_addr2.sin_port = htons(log_port);
	multicast_addr2.sin_addr.s_addr = htonl(INADDR_ANY);      << like this
	
	// disable receiving multicast messages we send
	a=0;
	setsockopt(sock.at(i), SOL_SOCKET, IP_MULTICAST_LOOP, &a, sizeof(a));
	*/
	
	// to listen we need to bind to the socket
	get_ok = (bind(socket_handle, (struct sockaddr*)&addr, addrlen) == 0);
	if(!get_ok) {
		LOG(logger,LOG_ERR,"Failed to bind to %s listen socket: ",type_str.c_str(),strerror(errno));
		return false;
	}
	
	// and join a multicast group
	struct ip_mreq mreq;
	get_ok = inet_aton(multicast_address.c_str(), &mreq.imr_multiaddr);
	if(get_ok==0){
		LOG(logger,LOG_ERR,"Bad %s multicast group '%s'",type_str.c_str(),multicast_address.c_str());
		return false;
	}
	// ...on all interfaces
	struct ifaddrs *ifaddr;
	if(getifaddrs(&ifaddr)==-1){
		LOG(logger,LOG_ERR,"%s getifaddrs couldn't find any network interfaces!",type_str.c_str());
		return false;
	}
	std::set<std::string> ifnames;
	for(struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next){
		
		if(ifa->ifa_addr == nullptr) continue; // ignore in case no IP address; e.g. if interface down/unconfigured
		
		int family = ifa->ifa_addr->sa_family;
		if(family!=AF_INET) continue; // ip_mreq is IPv4-only; treating an AF_INET6 sockaddr as sockaddr_in reads garbage bytes
		
		// only continue for the first address for an interface, since calling it multiple times gives 'address already in use'
		if(ifnames.count(ifa->ifa_name)) continue;
		
		mreq.imr_interface = ((sockaddr_in*)ifa->ifa_addr)->sin_addr;
		LOG(logger,LOG_NOTICE,"%s joining multicast group in interface %s",
		    type_str.c_str(),inet_ntoa(mreq.imr_interface));
		
		get_ok = setsockopt(socket_handle, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
		if(get_ok!=0){
			LOG(logger,LOG_ERR,"%s failed to join multicast group: %s",type_str.c_str(),strerror(errno));
			return false;
		}
		ifnames.emplace(ifa->ifa_name);
	}
	// we're responsible for freeing the list
	freeifaddrs(ifaddr);
	
	// we can also specify which interface outgoing multicast traffic should go to
	if(!out_interface_address.empty()){
		struct in_addr out_addr;
		get_ok = inet_aton(out_interface_address.c_str(), &out_addr);
		if(get_ok==0){
			LOG(logger,LOG_ERR,"Bad %s output interface '%s': %s",
			    type_str.c_str(),out_interface_address.c_str(),strerror(errno));
			return false;
		}
		if (setsockopt(socket_handle, IPPROTO_IP, IP_MULTICAST_IF, &out_addr,sizeof(out_addr))== -1) {
			LOG(logger,LOG_ERR,"Failed to set %s output interface '%s': %s",
			    type_str.c_str(),out_interface_address.c_str(),strerror(errno));
			return false;
		}
	}
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	// monitoring struct to encapsulate tracking info
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.m_data = m_data;
	thread_args.m_tool_name = m_tool_name;
	thread_args.monitoring_vars = &monitoring_vars;
	thread_args.socket = socket_handle;
	thread_args.addr = addr;
	thread_args.addrlen = addrlen;
	thread_args.poll = zmq::pollitem_t{NULL, socket_handle, ZMQ_POLLIN, 0};
	thread_args.poll_timeout_ms = poll_timeout_ms;
	thread_args.local_buffer_size = local_buffer_size;
	thread_args.in_local_queue = m_data->multicast_buffer_pool.GetNew(local_buffer_size);
	thread_args.in_local_queue->resize(0);
	thread_args.last_transfer = std::chrono::steady_clock::now();
	thread_args.transfer_period_ms = std::chrono::milliseconds{transfer_period_ms};
	thread_args.in_queue = &m_data->in_multicast_msg_queue;
	thread_args.in_queue_mtx = &m_data->in_multicast_msg_queue_mtx;
	if(type_str=="logging"){
		thread_args.out_queue = &m_data->out_log_msg_queue;
		thread_args.out_queue_mtx = &m_data->out_log_msg_queue_mtx;
	} else {
		thread_args.out_queue = &m_data->out_mon_msg_queue;
		thread_args.out_queue_mtx = &m_data->out_mon_msg_queue_mtx;
	}
	
	// thread needs a unique name
	LOG(logger,LOG_INFO,"spawning %s send/receiver thread",type_str.c_str());
	if(!m_data->utils.CreateThread(type_str+"_sendreceiver", &Thread, &thread_args)){
		LOG(logger,LOG_ERR,"Failed to spawn background %s thread",type_str.c_str());
		return false;
	}
	m_data->num_threads++;
	
	// pre-fill the Pool
	std::vector<std::string*> ms;
	for(int i=0; i<15; ++i){
		ms.emplace_back(m_data->multicast_batch_pool.GetNew());
		ms.back()->resize(local_buffer_size);
	}
	for(std::string* m : ms) m_data->multicast_batch_pool.Add(m);
	ms.clear();
	
	return true;
}


bool MulticastReceiverSender::Execute(){
	
	if(!thread_args.running){
		LOG(logger,LOG_ERR,"Execute found %s thread not running!",type_str.c_str());
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		// FIXME if restarts > X times in last Y mins, alarm (bypass, shove into DB? send to websocket?) and StopLoop.
		++(monitoring_vars.thread_crashes);
	}
	
	// Hmmm, throttling of the main thread is specified in the Sleep tool
	// that's toolchain-wide so is currently quite short, but means
	// these variables are being updated thousands of times a second.
	// Pro: monitoring info is up-to-date when it goes out
	// Con: wasteful...
	// FIX: update 1/10th monitoring interval? synchronise with Monitoring Tool?
	auto time_now = std::chrono::steady_clock::now();
	double ms_since_last = std::chrono::duration_cast<std::chrono::milliseconds>(time_now-last_bytes_time).count();
	if(ms_since_last<1000) return true;
	
	int64_t bytes_in = monitoring_vars.bytes_received.load();
	int64_t bytes_out = monitoring_vars.bytes_sent.load();
	double receive_rate_MBps = 1E-3*(bytes_in - last_bytes_in)/ms_since_last;
	double send_rate_MBps = 1E-3*(bytes_out - last_bytes_out)/ms_since_last;
	
	monitoring_vars.Set("buffered_in_messages",thread_args.in_local_queue->size());
	monitoring_vars.Set("waiting_out_messages",thread_args.out_local_queue.size());
	monitoring_vars.Set("receive_rate_MB/s",receive_rate_MBps);
	monitoring_vars.Set("send_rate_MB/s",send_rate_MBps);
	//printf("%-20s\treceive rate: %.0f MB/s\treceived: %.0f MB\ttransers: %d\n",type_str.c_str(),receive_rate_MBps,double(bytes_in)/1E6,monitoring_vars.in_buffer_transfers.load());
	
	last_bytes_time = time_now;
	last_bytes_in = bytes_in;
	last_bytes_out = bytes_out;
	
	/*
	actually we can't do this. steady_clock is what we want for regular tasks,
	but cannot be converted to a meaningful time unless we manually keep some
	reference time for conversion. Even then, it may drift as, by definition,
	it does not necessarily stay in sync with system_clock. Not sure what to do about that!
	// aren't you glad we have std::chrono to avoid all that c-style time jank?
	time_t lt = thread_args.last_transfer.time_since_epoch().count();
	struct tm lt_s;
	localtime_r(&lt, &lt_s);
	char tbuf[50];
	strftime(tbuf, 50, "%F %T%z",&lt_s);
	monitoring_vars.Set("last_transfer",tbuf);
	*/
	
	return true;
}


bool MulticastReceiverSender::Finalise(){
	
	// signal background receiver thread to stop
	LOG(logger,LOG_NOTICE,"Joining %s receiver thread",type_str.c_str());
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->in_multicast_msg_queue_mtx);
	m_data->in_multicast_msg_queue.clear();
	locker.unlock();
	
	if(type_str=="logging"){
		locker = std::unique_lock<std::mutex>(m_data->out_log_msg_queue_mtx);
		m_data->out_log_msg_queue.clear();
	} else {
		locker = std::unique_lock<std::mutex>(m_data->out_mon_msg_queue_mtx);
		m_data->out_mon_msg_queue.clear();
	}
	
	if(socket_handle>0){
		get_ok = close(socket_handle);
		if(get_ok!=0){
			LOG(logger,LOG_ERR,"Error closing %s socket: %s",type_str.c_str(),strerror(errno));
			return false;
		}
	}
	
	locker = std::unique_lock<std::mutex>(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	LOG(logger,LOG_NOTICE,"%s Finished",type_str.c_str());
	return true;
}

void MulticastReceiverSender::Thread(Thread_args* arg){
	
	MulticastReceive_args* m_args=reinterpret_cast<MulticastReceive_args*>(arg);
	DataModel* m_data = m_args->m_data;
	
	// transfer to datamodel
	// =====================
	if(!m_args->in_local_queue->empty() &&
	   ((m_args->in_local_queue->size()>m_args->local_buffer_size) ||
	    (std::chrono::steady_clock::now() - m_args->last_transfer) > m_args->transfer_period_ms) ){
		
		//printf("adding %d %s messages to datamodel\n",m_args->in_local_queue->size(), m_args->m_tool_name.c_str());
		
		std::unique_lock<std::mutex> locker(*m_args->in_queue_mtx);
		m_args->in_queue->push_back(m_args->in_local_queue);
		locker.unlock();
		
		m_args->in_local_queue = m_data->multicast_buffer_pool.GetNew(m_args->local_buffer_size);
		m_args->in_local_queue->resize(0);
		
		m_args->last_transfer = std::chrono::steady_clock::now();
		++(m_args->monitoring_vars->in_buffer_transfers);
	}
	
	
	// poll
	// ====
	try {
		m_args->get_ok = zmq::poll(&m_args->poll, 1, m_args->poll_timeout_ms);
	} catch(zmq::error_t& err){
		// ignore poll aborting due to signals
		if(zmq_errno()==EINTR) return;
		LOG(m_args->m_data->logger,LOG_WARNING,"%s poll caught %s",m_args->m_tool_name.c_str(),err.what());
		++(m_args->monitoring_vars->polls_failed);
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread?
		return;
	}
	catch(...){
		LOG(m_args->m_data->logger,LOG_WARNING,"%s poll caught %s",m_args->m_tool_name.c_str(),strerror(errno));
		++(m_args->monitoring_vars->polls_failed);
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		return;
	}
	if(m_args->get_ok<0){
		LOG(m_args->m_data->logger,LOG_WARNING,"%s poll failed with %s",m_args->m_tool_name.c_str(),zmq_strerror(errno));
		++(m_args->monitoring_vars->polls_failed);
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread?
		return;
	}
	
	// read
	// ====
	if(m_args->poll.revents & ZMQ_POLLIN){
		//printf("%s receiving message\n",m_args->m_tool_name.c_str());
		
		// read the messge
		m_args->get_ok = recvfrom(m_args->socket, m_args->message, MAX_UDP_PACKET_SIZE, 0, (struct sockaddr*)&m_args->addr, &m_args->addrlen);
		if(m_args->get_ok <= 0){
			++(m_args->monitoring_vars->rcv_fails);
			LOG(m_args->m_data->logger,LOG_ERR,"%s: Failed to receive message from %s with error %s",
			    m_args->m_tool_name.c_str(), inet_ntoa(m_args->addr.sin_addr), // FIXME is this valid on failure?
			    strerror(errno));
			// FIXME error handling - is this a socket error or just a message error?
		} else {
			
			++(m_args->monitoring_vars->msgs_rcvd);
			m_args->monitoring_vars->bytes_received += m_args->get_ok;
			// XXX
			//printf("received multicast message %s from %s\n",m_args->message,inet_ntoa(m_args->addr.sin_addr));
			
			m_args->in_local_queue->emplace_back(m_args->message,m_args->get_ok);
			
		}
	}
	
	// write
	// =====
	if(m_args->out_i < m_args->out_local_queue.size()){
		
		//printf("%s sending message\n",m_args->m_tool_name.c_str());
		
		// Get the message
		std::string& message = m_args->out_local_queue[m_args->out_i++]; // always increment, even if error
		
		// send it
		//printf("sending mon msg: '%s'\n",message.c_str());
		m_args->get_ok = sendto(m_args->socket, message.c_str(), message.length(), 0, (struct sockaddr*)&m_args->addr, m_args->addrlen);
		
		// check success
		if(m_args->get_ok < 0){
			LOG(m_args->m_data->logger,LOG_ERR,"Error sending %s message: %s",m_args->m_tool_name.c_str(),strerror(errno));
			++(m_args->monitoring_vars->send_fails);
		} else {
			++(m_args->monitoring_vars->msgs_sent);
			m_args->monitoring_vars->bytes_sent += m_args->get_ok;
		}
		
	} else {
		
		
		// else see if there are any in datamodel to grab
		std::unique_lock<std::mutex> locker(*m_args->out_queue_mtx);
		if(!m_args->out_queue->empty()){
			m_args->out_local_queue.clear();
			//printf("%s fetching new outgoing messages\n",m_args->m_tool_name.c_str());
			std::swap(*m_args->out_queue, m_args->out_local_queue);
			++(m_args->monitoring_vars->out_buffer_transfers);
			m_args->out_i=0;
		}
		locker.unlock();
		
	}
	
	return;
}
