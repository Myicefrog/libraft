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

int main(int argc, const char** argv) {
	struct tmp {
		int size;
		map<uint64_t, bool> votes;
		StateType state;
		tmp(int size, map<uint64_t, bool> votes, StateType s)
			: size(size), votes(votes), state(s) {}
	};

	vector<tmp> tests;
	// win the election when receiving votes from a majority of the servers
	{
		map<uint64_t, bool>	votes;
		tests.push_back(tmp(1, votes, StateLeader));
	}

	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		votes[3] = true;
		tests.push_back(tmp(3, votes, StateLeader));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		tests.push_back(tmp(3, votes, StateLeader));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		votes[3] = true;
		votes[4] = true;
		votes[5] = true;
		tests.push_back(tmp(5, votes, StateLeader));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		votes[3] = true;
		votes[4] = true;
		tests.push_back(tmp(5, votes, StateLeader));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		votes[3] = true;
		tests.push_back(tmp(5, votes, StateLeader));
	}
	// return to follower state if it receives vote denial from a majority
	{
		map<uint64_t, bool>	votes;
		votes[2] = false;
		votes[3] = false;
		tests.push_back(tmp(3, votes, StateFollower));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = false;
		votes[3] = false;
		votes[4] = false;
		votes[5] = false;
		tests.push_back(tmp(5, votes, StateFollower));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		votes[3] = false;
		votes[4] = false;
		votes[5] = false;
		tests.push_back(tmp(5, votes, StateFollower));
	}
	// stay in candidate if it does not obtain the majority
	{
		map<uint64_t, bool>	votes;
		tests.push_back(tmp(3, votes, StateCandidate));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = true;
		tests.push_back(tmp(5, votes, StateCandidate));
	}
	{
		map<uint64_t, bool>	votes;
		votes[2] = false;
		votes[3] = false;
		tests.push_back(tmp(5, votes, StateCandidate));
	}
	{
		map<uint64_t, bool>	votes;
		tests.push_back(tmp(5, votes, StateCandidate));
	}

	int i;
	for (i = 0; i < tests.size(); ++i) {
		cout<<"******"<<i<<"begin"<<endl;
		tmp &t = tests[i];

		vector<uint64_t> peers;
		idsBySize(t.size, &peers);
		Storage *s = new MemoryStorage(&kDefaultLogger);
		raft *r = newTestRaft(1, peers, 10, 1, s);
		
		{
			Message msg;
			msg.set_from(1);
			msg.set_to(1);
			msg.set_type(MsgHup);
			r->step(msg);
		}
		map<uint64_t, bool>::iterator iter;
		cout<<"votes size is "<<t.votes.size()<<endl;
		for (iter = t.votes.begin(); iter != t.votes.end(); ++iter) {
			Message msg;
			msg.set_from(iter->first);
			msg.set_to(1);
			msg.set_type(MsgVoteResp);
			msg.set_reject(!iter->second);
			r->step(msg);
		}

		cout<<"i: "<<i<<" r->state_ is "<<r->state_<<" t.state is "<<t.state<<endl;
		cout<<"r->term_ is "<<r->term_<<endl;
		cout<<"******"<<i<<"end"<<endl;
	}
}