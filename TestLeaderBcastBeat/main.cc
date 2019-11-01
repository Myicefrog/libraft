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
   	uint64_t hi = 1;
  vector<uint64_t> peers;
  peers.push_back(1);
  peers.push_back(2);
  peers.push_back(3);
  Storage *s = new MemoryStorage(&kDefaultLogger);
  raft *r = newTestRaft(1, peers, 10, 1, s);
	r->becomeCandidate();
	r->becomeLeader();

  EntryVec entries;
  int i;
  for (i = 0; i < 10; ++i) {
    Entry entry;
    entry.set_index(i + 1);
    entries.push_back(entry);
  }
  r->appendEntry(&entries);

  for (i = 0; i < hi; ++i) {
		r->tick();
	}
  vector<Message*> msgs;
  r->readMessages(&msgs);

	vector<Message*> wmsgs;
	{
		Message *msg = new Message();
		msg->set_from(1);
		msg->set_to(2);
		msg->set_term(1);
		msg->set_type(MsgHeartbeat);
		wmsgs.push_back(msg);
	}
	{
		Message *msg = new Message();
		msg->set_from(1);
		msg->set_to(3);
		msg->set_term(1);
		msg->set_type(MsgHeartbeat);
		wmsgs.push_back(msg);
	}

    bool ret = isDeepEqualMsgs(msgs, wmsgs);
    cout<<"ret is "<<ret<<endl;
}