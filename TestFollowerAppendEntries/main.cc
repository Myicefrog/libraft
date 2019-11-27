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

Message acceptAndReply(Message *msg) {
  Message m;
  m.set_from(msg->to());
  m.set_to(msg->from());
  m.set_term(msg->term());
  m.set_type(MsgAppResp);
  m.set_index(msg->index() + msg->entries_size());
  return m;
}

int main(int argc, char **argv)
{
	struct tmp {
		uint64_t index, term;
		EntryVec ents, wents, wunstable;

		tmp(uint64_t i, uint64_t t, EntryVec e, EntryVec we, EntryVec wu)
			: index(i), term(t), ents(e), wents(we), wunstable(wu) {}
	};

	vector<tmp> tests;
	{
		EntryVec ents, wents, wunstable;
		Entry entry;

		entry.set_term(3);
		entry.set_index(3);
		ents.push_back(entry);
		
		entry.set_term(1);
		entry.set_index(1);
		wents.push_back(entry);
		entry.set_term(2);
		entry.set_index(2);
		wents.push_back(entry);
		entry.set_term(3);
		entry.set_index(3);
		wents.push_back(entry);
	
		wunstable.push_back(entry);

		tests.push_back(tmp(2, 2, ents, wents, wunstable));
	}
	{
		EntryVec ents, wents, wunstable;
		Entry entry;

		entry.set_term(3);
		entry.set_index(2);
		ents.push_back(entry);
		entry.set_term(4);
		entry.set_index(3);
		ents.push_back(entry);
		
		entry.set_term(1);
		entry.set_index(1);
		wents.push_back(entry);
		entry.set_term(3);
		entry.set_index(2);
		wents.push_back(entry);
		wunstable.push_back(entry);

		entry.set_term(4);
		entry.set_index(3);
		wents.push_back(entry);
		wunstable.push_back(entry);

		tests.push_back(tmp(1, 1, ents, wents, wunstable));
	}
	{
		EntryVec ents, wents, wunstable;
		Entry entry;

		entry.set_term(1);
		entry.set_index(1);
		ents.push_back(entry);
		
		entry.set_term(1);
		entry.set_index(1);
		wents.push_back(entry);

		entry.set_term(2);
		entry.set_index(2);
		wents.push_back(entry);

		tests.push_back(tmp(0, 0, ents, wents, wunstable));
	}
	{
		EntryVec ents, wents, wunstable;
		Entry entry;

		entry.set_term(3);
		entry.set_index(1);
		ents.push_back(entry);
		wents.push_back(entry);
		wunstable.push_back(entry);

		tests.push_back(tmp(0, 0, ents, wents, wunstable));
	}

	int i;
	for (i = 0; i < tests.size(); ++i) {
		tmp& t = tests[i];
		vector<uint64_t> peers;
		peers.push_back(1);
		peers.push_back(2);
		peers.push_back(3);
		Storage *s = new MemoryStorage(&kDefaultLogger);

		EntryVec appEntries;
		Entry entry;
		entry.set_term(1);
		entry.set_index(1);
		appEntries.push_back(entry);
		entry.set_term(2);
		entry.set_index(2);
		appEntries.push_back(entry);

    s->Append(appEntries);
		raft *r = newTestRaft(1, peers, 10, 1, s);
		r->becomeFollower(2, 2);
    {
      Message msg;
      msg.set_type(MsgApp);
      msg.set_from(2);
      msg.set_to(1);
      msg.set_term(2);
      msg.set_logterm(t.term);
      msg.set_index(t.index);
			int j;
			for (j = 0; j < t.ents.size(); ++j) {
				*(msg.add_entries()) = t.ents[j];
			}

      r->step(msg);
    }
  }
}