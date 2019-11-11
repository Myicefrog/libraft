#include <unistd.h>
#include "libraft.h"
#include "util.h"
#include "raft.h"
#include "memory_storage.h"
#include "default_logger.h"
#include "progress.h"
#include "read_only.h"
#include "node.h"

#include "raft_test_util.h"

int main()
{
  	vector<stateMachine*> peers;

  	{
    	vector<uint64_t> ids;
    	ids.push_back(1);
    	ids.push_back(2);
    	ids.push_back(3);
    	raft *r = newTestRaft(1, ids, 10, 1, new MemoryStorage(&kDefaultLogger));
    	peers.push_back(new raftStateMachine(r)); 
  	}	
		

  	{
    	vector<uint64_t> ids;
    	ids.push_back(1);
    	ids.push_back(2);
    	ids.push_back(3);
    	raft *r = newTestRaft(2, ids, 10, 1, new MemoryStorage(&kDefaultLogger));
    	peers.push_back(new raftStateMachine(r)); 
  	}
	  
  	{
    	vector<uint64_t> ids;
    	ids.push_back(1);
    	ids.push_back(2);
    	ids.push_back(3);
    	raft *r = newTestRaft(3, ids, 10, 1, new MemoryStorage(&kDefaultLogger));
    	peers.push_back(new raftStateMachine(r)); 
  	}

	network *net = newNetwork(peers);

    cout<<"*********1 send**********"<<endl;
	{
    	vector<Message> msgs;
    	Message msg;
    	msg.set_from(1);
    	msg.set_to(1);
    	msg.set_type(MsgHup);
    	msgs.push_back(msg);
    	net->send(&msgs);
  	}
	sleep(2);

	cout<<"*********MsgBeat**********"<<endl;
	{	
		vector<Message> msgs;
		Message msg;
   		msg.set_from(1);
		msg.set_to(1);
    	msg.set_type(MsgBeat);
    	msgs.push_back(msg);
		net->send(&msgs);
	}

}
