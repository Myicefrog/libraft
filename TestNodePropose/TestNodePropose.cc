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
	vector<ReadState*> readStates;
	NodeImpl* n = new NodeImpl();
	n->logger_ = &kDefaultLogger;
	Storage *s = new MemoryStorage(&kDefaultLogger);
	vector<uint64_t> peers;
	peers.push_back(1);
	raft* r = newTestRaft(1, peers, 10, 1, s);
	n->raft_ = r;

	readStates.push_back(new ReadState(1, "somedata"));
	r->readStates_ = readStates;

	Ready* ready;
	n->Campaign(&ready);

	while(true)
	{
		s->Append(ready->entries);
		if(ready->softState.leader == r->id_)
		{
			n->Advance();
			break;
		}
		n->Advance();
	}
	r->stateStep = appendStep;
  	string wrequestCtx = "somedata2";
  	n->ReadIndex(wrequestCtx, &ready);
	
	return 0;
}
