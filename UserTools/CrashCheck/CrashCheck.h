#ifndef CrashCheck_H
#define CrashCheck_H

#include <string>
#include <iostream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class CrashCheck
*
* This tool creates a simple file containing the current time and date in Initialise and deletes that file in Finalise.
* If the tool finds an instance of that file in Initialise, this indicates a previous toolchain run did not terminate gracefully (Finalise did not run to clean up the file), and so logs it as a potential problem. If a user ctrl+C's the appilcation, this will lead to false positives. 
*
* $Author: Marcus O'Flaherty $
* $Date: 01/10/2026 $
*/

class CrashCheck: public Tool {


  public:

  CrashCheck(); ///< Simple constructor
  bool Initialise(std::string configfile,DataModel &data); ///< Initialise function for setting up Tool resources. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
  bool Execute(); ///< Execute function used to perform Tool purpose.
  bool Finalise(); ///< Finalise function used to clean up resources.


  private:
  std::string crash_check_file;




};


#endif
