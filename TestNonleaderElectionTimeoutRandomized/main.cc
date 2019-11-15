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
  raft *r = newTestRaft(1, peers, et, 1, s);
  int i;
  map<int, bool> timeouts;
  for (i = 0; i < 50 * et; ++i) {
    switch (state) {
    case StateFollower:
      r->becomeFollower(r->term_ + 1,2);
      break;
    case StateCandidate:
      r->becomeCandidate();
      break;
    }

    uint64_t time = 0;
    vector<Message*> msgs;
    r->readMessages(&msgs);
    while (msgs.size() == 0) {
      r->tick();
      time++;
      r->readMessages(&msgs);
      cout<<"i is "<<i<<" time is "<<time<<endl;
    }
  
    timeouts[time] = true;
  }

  for (i = et + 1; i < 2 * et; ++i) {
    cout<<"timeout is "<<timeouts[i]<<endl;
  }
}