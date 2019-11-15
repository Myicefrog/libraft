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

void commitNoopEntry(raft *r, Storage *s) {
  cout<<"commitNoopEntry"<<endl;
  r->bcastAppend();
  // simulate the response of MsgApp
  vector<Message*> msgs;
  r->readMessages(&msgs);

  int i;
  for (i = 0; i < msgs.size(); ++i) {
    Message *msg = msgs[i];
    r->step(acceptAndReply(msg));
  }
  // ignore further messages to refresh followers' commit index
  r->readMessages(&msgs);
  EntryVec entries;
  r->raftLog_->unstableEntries(&entries);
  s->Append(entries);
  r->raftLog_->appliedTo(r->raftLog_->committed_);
  r->raftLog_->stableTo(r->raftLog_->lastIndex(), r->raftLog_->lastTerm());
}

bool isDeepEqualMsgs(const vector<Message*>& msgs1, const vector<Message*>& msgs2) {
	if (msgs1.size() != msgs2.size()) {
    kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
		return false;
	}
	int i;
	for (i = 0; i < msgs1.size(); ++i) {
		Message *m1 = msgs1[i];
		Message *m2 = msgs2[i];
		if (m1->from() != m2->from()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->to() != m2->to()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "m1 to %llu, m2 to %llu", m1->to(), m2->to());
			return false;
		}
		if (m1->term() != m2->term()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->logterm() != m2->logterm()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->index() != m2->index()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->commit() != m2->commit()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->type() != m2->type()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->reject() != m2->reject()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
		if (m1->entries_size() != m2->entries_size()) {
      kDefaultLogger.Debugf(__FILE__, __LINE__, "error");
			return false;
		}
	}
	return true;
}

int main(int argc, const char** argv) {
  vector<uint64_t> peers;
  peers.push_back(1);
  peers.push_back(2);
  peers.push_back(3);
  Storage *s = new MemoryStorage(&kDefaultLogger);
  raft *r = newTestRaft(1, peers, 10, 1, s);
  r->becomeCandidate();
  r->becomeLeader();

  commitNoopEntry(r, s);
  uint64_t li = r->raftLog_->lastIndex();
  
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

{
  vector<Message*> msgs;
  r->readMessages(&msgs);
  int i;
  for (i = 0; i < msgs.size(); ++i) {
    Message *msg = msgs[i];
    r->step(acceptAndReply(msg));
  }
}

  cout<<"r->raftLog_->lastIndex() is "<<r->raftLog_->lastIndex()<<" li+1 is "<<li + 1<<endl;
  cout<<"r->raftLog_->committed_ is "<<r->raftLog_->committed_<<" li is "<<li<<endl;

}