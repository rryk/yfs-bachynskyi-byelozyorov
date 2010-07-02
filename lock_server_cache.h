#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

#include "rsm.h"


class lock_server_cache {
 private:
  class rsm *rsm;
 public:
  lock_server_cache(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
};

#endif
