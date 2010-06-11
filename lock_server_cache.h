#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <list>

#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

/** cache_lock_t class
 * 
 *  This class represents a single lock on a server that can be cached 
 *  by the client. It's purpose to provide functions that can safely 
 *  aquire and release lock in multithreading enviroment. Additionally
 *  it will keep the list of other clients that have previously 
 *  requested the lock to be able to notify them whenever the lock is
 *  successfully revoked.
 */
class cache_lock_t {
public:
	// This constructor is require to allow use of cache_lock_t as map element.
	// Since every lock must know it's own id, then this constructor should ever 
	// be used.
	cache_lock_t() { assert(false); }
	
	/** Constructor. Initalizes internal structures and sets "unlocked" state 
     *  for the lock.
	 *
	 *  @param lid Lock ID
	 */
    cache_lock_t(int lid);
    
    /** Aquires the lock. Pauses thread until lock is released.
	 *
	 *  @param addr Address for the RPC calls to the client
	 *  @return Result for the acquire operation -- could be RETRY or OK
	 */
    lock_protocol::status acquire(std::string addr);
    
    /// Releases the lock. Wakes up other thread waiting to aquire this lock.
    void release();
    
    /// Returns the current status of the lock.
    bool isLocked();
	
private:
	lock_protocol::lockid_t id; // current lock id
	std::string lockHolder; // address of the current lock holder (or an empty string if noone is holding the lock)
    pthread_cond_t okToLock; // condition on which acquire should be woken
    pthread_mutex_t mutex; // mutex to process multi-thread access to the variables
	std::list<std::string> interestedClients; // list of the clients that are waiting for this lock
};

class lock_server_cache {
public:
	/// This is constructor for the lock server. It will start revoker
	/// and retryer threads and set up internal variables to their initial
	/// values.
	lock_server_cache();
	
	/// This function is to be executed in a separate thread and
	/// it will process revoke requests in a continuous loop.
	void revoker();
	
	/// This function is to be executed in a separate thread and
	/// it will process retry requests in a continuous loop.
	void retryer();
  
	/** Get status of a certain lock (RPC command handler)
	 *
	 *  @param clt Client ID
	 *  @param lid Lock ID
	 *  @param r Result of the stat operation -- 1 if locked, 0 if unlocked
	 *  @return Execution status of the RPC function. Indicates success or
	 *          failure code.
	 */
	lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int & r);

	/** Aquire certain lock (RPC command handler)
	 *
	 *  @param clt Client ID
	 *  @param lid Lock ID
	 *  @param rpc_addr Address for the RPC calls to the client
	 *  @param r Result for the acquire operation -- could be RETRY or OK
	 *  @return Execution status of the RPC function. Indicates success or
	 *          failure code.
	 */
	lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, std::string rpc_addr, int & r);

	/** Release certain lock (RPC command handler)
	 *
	 *  @param clt Client ID
	 *  @param lid Lock ID
	 *  @return Execution status of the RPC function. Indicates success or
	 *          failure code.
	 */
	lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
	
protected:
    std::map<lock_protocol::lockid_t, cache_lock_t> locks; // lock map
	pthread_mutex_t mutex; // mutex to protect map modification (such as adding new unknown-before locks)
};

#endif
