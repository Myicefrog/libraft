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
    EntryVec entries;
    uint64_t commit;
    
    tmp(EntryVec ents, uint64_t commit)
      : entries(ents), commit(commit) {
    }
  };

  vector<tmp> tests;
  {
    Entry entry;
    EntryVec entries;

    entry.set_term(1);
    entry.set_index(1);
    entry.set_data("some data");
    entries.push_back(entry);

    tests.push_back(tmp(entries, 1));
  }
  {
    Entry entry;
    EntryVec entries;

    entry.set_term(1);
    entry.set_index(1);
    entry.set_data("some data");
    entries.push_back(entry);

    entry.set_term(1);
    entry.set_index(2);
    entry.set_data("some data2");
    entries.push_back(entry);

    tests.push_back(tmp(entries, 2));
  }
  {
    Entry entry;
    EntryVec entries;

    entry.set_term(1);
    entry.set_index(1);
    entry.set_data("some data2");
    entries.push_back(entry);

    entry.set_term(1);
    entry.set_index(2);
    entry.set_data("some data");
    entries.push_back(entry);

    tests.push_back(tmp(entries, 2));
  }
  {
    Entry entry;
    EntryVec entries;

    entry.set_term(1);
    entry.set_index(1);
    entry.set_data("some data");
    entries.push_back(entry);

    entry.set_term(1);
    entry.set_index(2);
    entry.set_data("some data2");
    entries.push_back(entry);

    tests.push_back(tmp(entries, 1));
  }

  int i;
  for (i = 0; i < tests.size(); ++i) {
    tmp &t = tests[i];

    vector<uint64_t> peers;
    peers.push_back(1);
    peers.push_back(2);
    peers.push_back(3);
    Storage *s = new MemoryStorage(&kDefaultLogger);
    raft *r = newTestRaft(1, peers, 10, 1, s);
    r->becomeFollower(1,2);

    {
      Message msg;
      msg.set_from(2);
      msg.set_to(1);
      msg.set_term(1);
      msg.set_type(MsgApp);
      msg.set_commit(t.commit);
      int j;
      for (j = 0; j < t.entries.size(); ++j) {
        *(msg.add_entries()) = t.entries[j];
      }
      r->step(msg);
    }

    cout<<r->raftLog_->committed_<<" "<<t.commit<<endl;
    EntryVec ents, wents;
    r->raftLog_->nextEntries(&ents);
    wents.insert(wents.end(), t.entries.begin(), t.entries.begin() + t.commit);
  }
}