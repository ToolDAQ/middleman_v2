#include "CrashCheck.h"
#include <fstream>
#include <cstdio>
#include <time.h>

CrashCheck::CrashCheck():Tool(){}


bool CrashCheck::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  //m_variables.Print();
  crash_check_file="/tmp/middleman_running";
  m_variables.Get("crash_check_file",crash_check_file);
  
  // if there's a crash check file that didn't get cleaned up, the last run didn't terminate gracefully
  LOG(m_data->logger,LOG_NOTICE,"checking for crash file %s",crash_check_file.c_str());
  std::ifstream f_check(crash_check_file.c_str());
  if(f_check.is_open()){
    std::string ts;
    f_check >> ts;
    LOG(m_data->logger,LOG_ERR,"Last middleman run started at %s did not terminate gracefully!",ts.c_str());
    f_check.close();
  }
  std::remove(crash_check_file.c_str());
  
  std::ofstream f_running(crash_check_file.c_str());
  if(!f_running.is_open()){
    LOG(m_data->logger,LOG_ERR,"Failed to create crash_check file %s", crash_check_file.c_str());
  } else {
    time_t rawtime = time(nullptr);
    f_running << ctime(&rawtime);
    f_running.close();
  }

  ExportConfiguration();

  return true;
}


bool CrashCheck::Execute(){

  return true;
}


bool CrashCheck::Finalise(){
  std::remove(crash_check_file.c_str());
  return true;
}
