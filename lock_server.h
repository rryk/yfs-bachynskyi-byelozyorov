// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_t {
public:
    lock_t();
    void acquire();
    void release();
    bool isLocked();

private:
    bool locked;
    pthread_cond_t okToLock;
    pthread_mutex_t mutex;
};

class lock_server {

 protected:
  int nacquire;

  std::map<lock_protocol::lockid_t, lock_t> locks;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







