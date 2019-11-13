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

	struct tmp {
    uint64_t vote;
    uint64_t nvote;
    bool wreject;
    tmp(uint64_t vote, uint64_t nvote, bool reject)
      : vote(vote), nvote(nvote), wreject(reject) {
    }
	};

	vector<tmp> tests;
  tests.push_back(tmp(None, 1, false));
  tests.push_back(tmp(None, 2, false));
  tests.push_back(tmp(1, 1, false));
  tests.push_back(tmp(2, 2, false));
  tests.push_back(tmp(1, 2, true));
  tests.push_back(tmp(2, 1, true));

  int i;
  for (i = 0; i < tests.size(); ++i) {
    tmp &t = tests[i];
    
    vector<uint64_t> peers;
    peers.push_back(1);
    peers.push_back(2);
    peers.push_back(3);
    Storage *s = new MemoryStorage(&kDefaultLogger);
    raft *r = newTestRaft(1, peers, 10, 1, s);

    HardState hs;
    hs.set_term(1);
    hs.set_vote(t.vote);
    r->loadState(hs);

		{
			Message msg;
			msg.set_from(t.nvote);
			msg.set_to(1);
			msg.set_type(MsgVote);
			r->step(msg);
		}

    vector<Message*> msgs;
    r->readMessages(&msgs);

    vector<Message*> wmsgs;
    {
      Message *msg = new Message();
      msg->set_from(1);
      msg->set_to(t.nvote);
      msg->set_term(1);
      msg->set_type(MsgVoteResp);
      msg->set_reject(t.wreject);
      wmsgs.push_back(msg);
    }
  }
}