// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};


class client_lock_t {
public:
  enum lock_status_t { NONE, FREE, LOCKED, ACQUIRING, RELEASING };

    /// Constructor. Initalizes internal structures and sets "NONE" state
    /// for the lock.
    client_lock_t();

    /// Aquires the lock. Pauses thread until lock is FREE. If there is no lock on client "NONE", sets lock to ACQUIRING
    /// and returns NONE. Could return NONE or FREE, depends on previous state
    int acquire();

    /// Releases the lock. Setting lock to RELEASING status
    int release();

    /// Setting lock to NONE status after RELEASING. Wakes up other thread waiting to aquire this lock, that has to
    /// reacquire it from server.
    void released();

    /// Setting lock to LOCKED after ACQUIRING
    void locked();

    /// Releases the lock locally. Set it to FREE. Wakes up other thread waiting to aquire this lock.
    void unlocked();

    /// Returns the current status of the lock. Never used :D
    int status();

private:
    int lockStatus;
    pthread_cond_t okToLock;
    pthread_mutex_t mutex;
};



// SUGGESTED LOCK CACHING IMPLEMENTATION PLAN:
//
// to work correctly for lab 7,  all the requests on the server run till 
// completion and threads wait on condition variables on the client to
// wait for a lock.  this allows the server to be replicated using the
// replicated state machine approach.
//
// On the client a lock can be in several states:
//  - free: client owns the lock and no thread has it
//  - locked: client owns the lock and a thread has it
//  - acquiring: the client is acquiring ownership
//  - releasing: the client is releasing ownership
//
// in the state acquiring and locked there may be several threads
// waiting for the lock, but the first thread in the list interacts
// with the server and wakes up the threads when its done (released
// the lock).  a thread in the list is identified by its thread id
// (tid).
//
// a thread is in charge of getting a lock: if the server cannot grant
// it the lock, the thread will receive a retry reply.  at some point
// later, the server sends the thread a retry RPC, encouraging the client
// thread to ask for the lock again.
//
// once a thread has acquired a lock, its client obtains ownership of
// the lock. the client can grant the lock to other threads on the client 
// without interacting with the server. 
//
// the server must send the client a revoke request to get the lock back. this
// request tells the client to send the lock back to the
// server when the lock is released or right now if no thread on the
// client is holding the lock.  when receiving a revoke request, the
// client adds it to a list and wakes up a releaser thread, which returns
// the lock the server as soon it is free.
//
// the releasing is done in a separate a thread to avoid
// deadlocks and to ensure that revoke and retry RPCs from the server
// run to completion (i.e., the revoke RPC cannot do the release when
// the lock is free.
//
// a challenge in the implementation is that retry and revoke requests
// can be out of order with the acquire and release requests.  that
// is, a client may receive a revoke request before it has received
// the positive acknowledgement on its acquire request.  similarly, a
// client may receive a retry before it has received a response on its
// initial acquire request.  a flag field is used to record if a retry
// has been received.
//


class lock_client_cache : public lock_client {
 private:
  rsm_client *rcl;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  /// id for creation of sockaddr obj, "ip:port"
  std::string id;

  long long unsigned int lastRequest;
  pthread_mutex_t mutex;

  /// Map for local saving locks
  std::map<lock_protocol::lockid_t, client_lock_t> localLocks;

  /// List of locks, that should be revoked it's mutex and condition variable for revoker thread
  std::list<lock_protocol::lockid_t> revokeList;
  pthread_cond_t okToRevoke;
  pthread_mutex_t mutexRevokeList;

  /// List of locks, that should be revoked by owner, it's mutex
  std::list<lock_protocol::lockid_t> revokeListByOwner;
  pthread_mutex_t mutexRevokeListByOwner;

  /// Map for saving flag, whether client received rpc for retry. Lock and condition variable for retrying to acquire lock from server
  std::map<lock_protocol::lockid_t,bool> retryMap;
  pthread_cond_t okToRetry;
  pthread_mutex_t mutexRetryMap;

 public:

  /// Last used port on this computer
  static int last_port;

  /// Constructor of lock_client_cache. xdst - string for creating sever socket connection "ip:port"
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  ~lock_client_cache();
  lock_protocol::status acquire(lock_protocol::lockid_t);

  /// Release lock, locally or to server if needed, signal to other threads waiting for that lock
  virtual lock_protocol::status release(lock_protocol::lockid_t);

  /// Revoke locks, which are FREE
  void releaser();

  /// RPC, that signals for waiting thread to retry to request lock from server
  rlock_protocol::status retry(lock_protocol::lockid_t lid, int &);

  /// RPC that signals, that lock should be revoked
  rlock_protocol::status revoke(lock_protocol::lockid_t lid, int &);
};
#endif
