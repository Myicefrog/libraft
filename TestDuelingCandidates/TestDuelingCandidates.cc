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

  	net->cut(1,3);

	{
    	vector<Message> msgs;
    	Message msg;
    	msg.set_from(1);
    	msg.set_to(1);
    	msg.set_type(MsgHup);
    	msgs.push_back(msg);
    	net->send(&msgs);
  	}
  	{
    	vector<Message> msgs;
    	Message msg;
    	msg.set_from(3);
    	msg.set_to(3);
    	msg.set_type(MsgHup);
    	msgs.push_back(msg);
    	net->send(&msgs);
 	}

	raft *r = (raft*)net->peers[1]->data();
	cout<<"1 become leader "<<r->state_<<" "<<StateLeader<<endl;

	r = (raft*)net->peers[3]->data();
	cout<<"3 become candidate "<<r->state_<<" "<<StateCandidate<<endl;

	net->recover();

	{
    	vector<Message> msgs;
    	Message msg;
    	msg.set_from(3);
    	msg.set_to(3);
    	msg.set_type(MsgHup);
    	msgs.push_back(msg);
    	net->send(&msgs);
  	}

  	MemoryStorage *s = new MemoryStorage(&kDefaultLogger); 
  	{
    	EntryVec entries;
    	entries.push_back(Entry());
    	Entry entry;
    	entry.set_term(1);
    	entry.set_index(1);
    	entries.push_back(entry);

    	s->Append(entries);
  	}

  	raftLog *log = new raftLog(s, &kDefaultLogger);
  	log->committed_ = 1;
  	log->unstable_.offset_ = 2;

  	struct tmp {
    	raft *r;
    	StateType state;
    	uint64_t term;
    	raftLog* log;

    	tmp(raft *r, StateType state, uint64_t term, raftLog *log)
      		: r(r), state(state), term(term), log(log) {}
  	};

  	vector<tmp> tests;
  	tests.push_back(tmp((raft*)peers[0]->data(), StateFollower, 2, log)); 
  	tests.push_back(tmp((raft*)peers[1]->data(), StateFollower, 2, log)); 
  	tests.push_back(tmp((raft*)peers[2]->data(), StateFollower, 2, new raftLog(new MemoryStorage(&kDefaultLogger), &kDefaultLogger))); 

  	int i;
  	for (i = 0; i < tests.size(); ++i) 
	{
    	tmp& t = tests[i];
    	cout<<"state "<<t.r->state_<<" "<<t.state<<endl;;
    	cout<<"term "<<t.r->term_<<" "<<t.term;

    	string base = raftLogString(t.log);
    	if (net->peers[i + 1]->type() == raftType) 
		{
      		raft *r = (raft*)net->peers[i + 1]->data();
      		string str = raftLogString(r->raftLog_);
      		cout<<"base "<<base<<" "<<str<< " i: " << i;
    	}
  	}
	return 0;
}
