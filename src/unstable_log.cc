#include "unstable_log.h"

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
bool unstableLog::maybeFirstIndex(uint64_t *first) {
  if (snapshot_ != NULL) {
    *first = snapshot_->metadata().index() + 1;
    return true;
  }

  *first = 0;
  return false;
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
bool unstableLog::maybeLastIndex(uint64_t *last) {
  *last = 0; 
  if (entries_.size() > 0) {
    *last = offset_ + entries_.size() - 1;
    return true;
  }

  if (snapshot_ != NULL) {
    *last = snapshot_->metadata().index();
    return true;
  }

  return false;
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
bool unstableLog::maybeTerm(uint64_t i, uint64_t *term) {
  *term = 0;
  if (i < offset_) {
    if (snapshot_ == NULL) {
      return false;
    }
    if (snapshot_->metadata().index() == i) {
      *term = snapshot_->metadata().term();
      return true;
    }

    return false;
  }

  uint64_t last;
  bool ok = maybeLastIndex(&last);
  cout<<"maybeLastIndex last is "<<last<<endl;
  if (!ok) {
    return false;
  }
  if (i > last) {
    return false;
  }
  *term = entries_[i - offset_].term();
  cout<<"term is "<<*term<<endl;
  return true;
}

void unstableLog::stableTo(uint64_t i, uint64_t t) {
  uint64_t gt;
  bool ok = maybeTerm(i, &gt);
  if (!ok) {
    return;
  }
  cout<<"unstableLog::stableTo is i "<<i<<" t is "<<t<<" gt is "<<gt<<" offset_ is "<<offset_<<endl;
  // if i < offset, term is matched with the snapshot
  // only update the unstable entries if term is matched with
  // an unstable entry.
  if (gt == t && i >= offset_) {
    entries_.erase(entries_.begin(), entries_.begin() + i + 1 - offset_);
    offset_ = i + 1;
    //logger_->Debugf(__FILE__, __LINE__, "stable to %llu, entries size:%d, offset:%llu", i, entries_.size(), offset_);
  }
}

void unstableLog::stableSnapTo(uint64_t i) {
  if (snapshot_ != NULL && snapshot_->metadata().index() == i) {
    delete snapshot_;
    snapshot_ = NULL;
  }
}

void unstableLog::restore(const Snapshot& snapshot) {
  offset_ = snapshot.metadata().index() + 1;
  entries_.clear();
  if (snapshot_ == NULL) {
    snapshot_ = new Snapshot();
  }
  snapshot_->CopyFrom(snapshot);
}

void unstableLog::truncateAndAppend(const EntryVec& entries) {
  uint64_t after = entries[0].index();
  cout<<"unstableLog::truncateAndAppend after is "<<after<<" offset_ is "<<offset_<<endl;
  cout<<"unstableLog::truncateAndAppend entries size is "<<entries.size()<<" entries_size is "<<entries_.size()<<endl;
  if (after == offset_ + uint64_t(entries_.size())) {
    // after is the next index in the u.entries
    // directly append
    entries_.insert(entries_.end(), entries.begin(), entries.end());
    logger_->Infof(__FILE__, __LINE__, "directly append ENTRY size: %d", entries_.size());
    return;
  }

  if (after <= offset_) {
    // The log is being truncated to before our current offset
    // portion, so set the offset and replace the entries
    logger_->Infof(__FILE__, __LINE__, "replace the unstable entries from index %llu", after);
    offset_ = after;
    entries_ = entries;
    return;
  }

  // truncate to after and copy to u.entries then append
  logger_->Infof(__FILE__, __LINE__, "truncate the unstable entries before index %llu", after);
  vector<Entry> slice;
  this->slice(offset_, after, &slice);
  entries_ = slice;
  entries_.insert(entries_.end(), entries.begin(), entries.end());
}

void unstableLog::slice(uint64_t lo, uint64_t hi, EntryVec *entries) {
  mustCheckOutOfBounds(lo, hi);
  entries->assign(entries_.begin() + lo - offset_, entries_.begin() + hi - offset_);
}

// u.offset <= lo <= hi <= u.offset+len(u.offset)
void unstableLog::mustCheckOutOfBounds(uint64_t lo, uint64_t hi) {
  if (lo > hi) {
    logger_->Fatalf(__FILE__, __LINE__, "invalid unstable.slice %llu > %llu", lo, hi);
  }

  uint64_t upper = offset_ + (uint64_t)entries_.size();
  if (lo < offset_ || upper < hi) {
    logger_->Fatalf(__FILE__, __LINE__, "unstable.slice[%llu,%llu) out of bound [%llu,%llu]", lo, hi, offset_, upper);
  }
}
