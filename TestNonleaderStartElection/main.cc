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

int main(int argc, char **argv)
{
  StateType state;
 if(string(argv[1]) == "0" )
 {
	 state = StateType(0);
 }
 else if(string(argv[1]) == "1")
 {
	 state = StateType(1);
 }
 else if(string(argv[1]) == "2")
 {
	 state = StateType(2);
 }
 else
 {
	 state = StateType(0);
 }
  uint64_t et = 10;
  vector<uint64_t> peers;
  peers.push_back(1);
  peers.push_back(2);
  peers.push_back(3);
  Storage *s = new MemoryStorage(&kDefaultLogger);
  raft *r = newTestRaft(1, peers, 10, 1, s);

  switch (state) {
	case StateFollower:
		r->becomeFollower(1,2);
		break;
	case StateCandidate:
		r->becomeCandidate();
		break;
  }
  int i;
  for (i = 0; i < 2*et; ++i) {
	r->tick();
  }

  cout<<"term_ 2 is "<<r->term_<<endl;
  cout<<"state_ candidate is "<<r->state_<<endl;
  cout<<"votes true is "<<r->votes_[r->id_]<<endl;

  vector<Message*> msgs;
  r->readMessages(&msgs);

  vector<Message*> wmsgs;
  {
	Message *msg = new Message();
	msg->set_from(1);
	msg->set_to(2);
	msg->set_term(2);
	msg->set_type(MsgVote);
	wmsgs.push_back(msg);
  }
  {
	Message *msg = new Message();
	msg->set_from(1);
	msg->set_to(3);
	msg->set_term(2);
	msg->set_type(MsgVote);
	wmsgs.push_back(msg);
  }
}