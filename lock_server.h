// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

/** lock_t class
 * 
 *  This class represents a single lock on a server. It's purpose to provide 
 *  functions that can safely aquire and release lock in multithreading 
 *  enviroment.
 */
  
class lock_t {
public:
    /// Constructor. Initalizes internal structures and sets "unlocked" state 
    /// for the lock.
    lock_t();
    
    /// Aquires the lock. Pauses thread until lock is released.
    void acquire();
    
    /// Releases the lock. Wakes up other thread waiting to aquire this lock.
    void release();
    
    /// Returns the current status of the lock.
    bool isLocked();

private:
    bool locked;
    pthread_cond_t okToLock;
    pthread_mutex_t mutex;
};

/** lock_server class
 * 
 *  This class provides services to clients via RPC. Current implementation
 *  provides basic lock managment.
 */

class lock_server {

 protected:
  int nacquire;

  std::map<lock_protocol::lockid_t, lock_t> locks;

 public:
  lock_server();
  ~lock_server() {};
  
  /** Get status of a certain lock
   *
   *  @param clt Client ID
   *  @param lid Lock ID
   *  @return Execution status of the RPC function. Indicates success or
   *          failure code.
   */
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  
  /** Aquire certain lock
   *
   *  @param clt Client ID
   *  @param lid Lock ID
   *  @return Execution status of the RPC function. Indicates success or
   *          failure code.
   */
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  
  /** Release certain lock
   *
   *  @param clt Client ID
   *  @param lid Lock ID
   *  @return Execution status of the RPC function. Indicates success or
   *          failure code.
   */
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







