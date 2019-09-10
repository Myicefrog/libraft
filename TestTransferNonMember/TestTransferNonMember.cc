#include "libraft.h"
#include "util.h"
#include "raft.h"
#include "memory_storage.h"
#include "default_logger.h"
#include "progress.h"
#include "read_only.h"
#include "node.h"

#include "raft_test_util.h"

vector<Message> msgs;

static void appendStep(raft *, const Message &msg) {
  msgs.push_back(Message(msg));
}

int main()
{
	vector<uint64_t> peers;
	peers.push_back(2);
  	peers.push_back(3);
  	peers.push_back(4);
  	Storage *s = new MemoryStorage(&kDefaultLogger);
  	raft *r = newTestRaft(1, peers, 5, 1, s);

  	{
    Message msg;
    msg.set_from(2);
    msg.set_to(1);
    msg.set_type(MsgTimeoutNow);
    r->step(msg);
  }

  {
    Message msg;
    msg.set_from(2);
    msg.set_to(1);
    msg.set_type(MsgVoteResp);
    r->step(msg);
  }
  {
    Message msg;
    msg.set_from(3);
    msg.set_to(1);
    msg.set_type(MsgVoteResp);
    r->step(msg);
  }
}
