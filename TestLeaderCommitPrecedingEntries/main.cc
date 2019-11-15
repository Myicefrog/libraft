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
  vector<EntryVec> tests;
  {
    EntryVec entries; 
    tests.push_back(entries);
  }
  {
    Entry entry;
    EntryVec entries; 
    entry.set_term(2);
    entry.set_index(1);
    entries.push_back(entry);
    tests.push_back(entries);
  }
  {
    Entry entry;
    EntryVec entries; 
    entry.set_term(1);
    entry.set_index(1);
    entries.push_back(entry);

    entry.set_term(2);
    entry.set_index(2);
    entries.push_back(entry);
    tests.push_back(entries);
  }
  {
    Entry entry;
    EntryVec entries; 
    entry.set_term(1);
    entry.set_index(1);
    entries.push_back(entry);

    tests.push_back(entries);
  }
  int i;
  for (i = 0; i < tests.size(); ++i) {
    cout<<"------------"<<i<<"---------------"<<endl;
    EntryVec &t = tests[i];
    vector<uint64_t> peers;
    peers.push_back(1);
    peers.push_back(2);
    peers.push_back(3);
    Storage *s = new MemoryStorage(&kDefaultLogger);
    EntryVec appEntries = t;
    s->Append(appEntries);
    raft *r = newTestRaft(1, peers, 10, 1, s);
    HardState hs;
    hs.set_term(2);
    r->loadState(hs);
		r->becomeCandidate();
		r->becomeLeader();

    {
      Entry entry;
      entry.set_data("some data");
      Message msg;
      msg.set_from(1);
      msg.set_to(1);
      msg.set_type(MsgProp);
      *(msg.add_entries()) = entry;
      r->step(msg);
    }

    vector<Message*> msgs;
    r->readMessages(&msgs);
    int j;
    for (j = 0; j < msgs.size(); ++j) {
      Message *msg = msgs[j];
      r->step(acceptAndReply(msg));
    }

    uint64_t li = t.size();
    EntryVec g, wents = t;
    {
      Entry entry;
      entry.set_term(3);
      entry.set_index(li + 1);
      wents.push_back(entry);
    }
    {
      Entry entry;
      entry.set_term(3);
      entry.set_index(li + 2);
      entry.set_data("some data");
      wents.push_back(entry);
    }
    r->raftLog_->nextEntries(&g);
  }
}