#include "Factory.h"

Tool* Factory(std::string tool) {
Tool* ret=0;

// if (tool=="Type") tool=new Type;
if (tool=="DummyTool") ret=new DummyTool;
if (tool=="MulticastReceiverSender") ret=new MulticastReceiverSender;
if (tool=="MulticastWorkers") ret=new MulticastWorkers;
if (tool=="DatabaseWorkers") ret=new DatabaseWorkers;
if (tool=="WriteQueryReceiver") ret=new WriteQueryReceiver;
if (tool=="ReadQueryReceiverReplySender") ret=new ReadQueryReceiverReplySender;
if (tool=="WriteWorkers") ret=new WriteWorkers;
if (tool=="Monitoring") ret=new Monitoring;
if (tool=="SocketManager") ret=new SocketManager;
if (tool=="ResultWorkers") ret=new ResultWorkers;
if (tool=="JobManager") ret=new JobManager;
//if (tool=="QueueTrimmer") ret=new QueueTrimmer;
//if (tool=="MiddlemanNegotiate") ret=new MiddlemanNegotiate;
if (tool=="Sleep") ret=new Sleep;
if (tool=="ReadWorkers") ret=new ReadWorkers;
if (tool=="TestAlerts") ret=new TestAlerts;
if (tool=="StopQuitFile") ret=new StopQuitFile;
if (tool=="GracefulStop") ret=new GracefulStop;
if (tool=="SetVerbosity") ret=new SetVerbosity;
if (tool=="CrashCheck") ret=new CrashCheck;
return ret;
}
