// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  // Seek random generator of first client to 1, all other to random value
  srand(time(NULL)^last_port);

  // Generating random port number to listen revoke and retry rpc from server
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;

  // Creating id for sockaddr "ip:port"
  id = host.str();
  last_port = rlock_port;

  // Creating new rpc server on port rlock_port
  rpcs *rlsrpc = new rpcs(rlock_port);

  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);

  // Creating new thread releaser, which runs method releaser()
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);

  // initialize condition vars and mutexes
  pthread_cond_init(&okToRetry, NULL);
  pthread_cond_init(&okToRevoke, NULL);
  pthread_mutex_init(&mutexRetryMap, NULL);
  pthread_mutex_init(&mutexRevokeList, NULL);
}

lock_client_cache::~lock_client_cache()
{
    pthread_mutex_lock(&mutexRevokeList);
    for (std::map<lock_protocol::lockid_t,client_lock_t>::iterator it=localLocks.begin();it!=localLocks.end();it++)
        revokeList.push_back((*it).first);
    pthread_mutex_unlock(&mutexRevokeList);
    pthread_cond_signal(&okToRevoke);
}

void
lock_client_cache::releaser()
{
    int r;

    while (true)
    {
        // Lock revokeList
        pthread_mutex_lock(&mutexRevokeList);

            // Sleep, while revokelist is empty
            while (revokeList.empty())
                    pthread_cond_wait(&okToRevoke, &mutexRevokeList);

            // Get id for revoking from revokeList and delete it there
            lock_protocol::lockid_t lid=revokeList.front();

            printf("lock_client_cache::releaser\n");

        // Unlock revokeList
        pthread_mutex_unlock(&mutexRevokeList);

            // Acquire lock, that should be revoked locally it should be FREE or NONE
            int acqRes=localLocks[lid].acquire();
            assert(acqRes==client_lock_t::FREE || acqRes==client_lock_t::NONE);

            // If it is FREE, we release it on server
            if (acqRes==client_lock_t::FREE)
            {
                localLocks[lid].release();
                lock_protocol::status rs=cl->call(lock_protocol::release, cl->id(), lid, r);

                // If release on server succeds, we change status of lock to NONE
                if (rs==lock_protocol::OK)
                {
                    localLocks[lid].released();
                    pthread_mutex_lock(&mutexRevokeList);
                        revokeList.pop_front();
                    pthread_mutex_unlock(&mutexRevokeList);
                }

                // Else we add our lockID back to revokeList, and Unlock lock locally, to be used by other thread, which needs it
                // and  then try to revoke it one more time
                else
                {
                        localLocks[lid].unlocked();
                }
            }

            // If status is NONE, lock is already released to server by other thread. We just change its localState to NONE
            // and signal to other thread, that needs it
            else
                localLocks[lid].released();
    }
}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    printf("lock_client_cache::acquire(%llu)\n", lid);
    // If lock was FREE, we can simply use it
    if(localLocks[lid].acquire()==client_lock_t::FREE)
        return lock_protocol::OK;

    // Else we should acquire it from server and when acquired, change its state to LOCKED
    else
    {
        int r;
        // Before call to server we set value in retryMap to false. If retry RPC will be delivered earlier as RETRY answer,
        // we just use this flag and don't wait.
        pthread_mutex_lock(&mutexRetryMap);
        retryMap[lid]=false;
        pthread_mutex_unlock(&mutexRetryMap);

        // Request to server
        lock_protocol::status as=cl->call(lock_protocol::acquire, cl->id(), lid, id, r);

        // If answer is RETRY, we retry to request it after we receive retry rpc, or immediately, if we received it
        while (as==lock_protocol::RETRY)
        {
            pthread_mutex_lock(&mutexRetryMap);
            while (!retryMap[lid])
                pthread_cond_wait(&okToRetry, &mutexRetryMap);
            retryMap[lid]=false;
            as=cl->call(lock_protocol::acquire, cl->id(), lid, id, r);
            pthread_mutex_unlock(&mutexRetryMap);
        }

        // If we received OK, so we have lock, change its status to LOCKED, and use it
        if (as==lock_protocol::OK)
            localLocks[lid].locked();

        // Else was some ERROR
        else localLocks[lid].released();

        // return that value
        return as;
    }
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    printf("lock_client_cache::release(%llu)\n", lid);
    lock_protocol::status rs;
    bool toRevoke=false;

    // We scan toRevoke list, whether we should release lock locally, or remotely. If we found such value we remove it from list
    // and set flag, that we should revoke it
    pthread_mutex_lock(&mutexRevokeList);
        for(std::list<lock_protocol::lockid_t>::iterator it=revokeList.begin();it!=revokeList.end();it++)
            if (*it==lid)
            {
                it=revokeList.erase(it);
                toRevoke=true;
                break;
            }
    pthread_mutex_unlock(&mutexRevokeList);

    // We try to revoke it remotely
    if(toRevoke)
    {
        int r;

        // Set value of local lock to releasing
        localLocks[lid].release();

        // Call release to server
        rs=cl->call(lock_protocol::release, cl->id(), lid, r);

        // If server released it properly, we change it local status to NONE
        if (rs==lock_protocol::OK)
        {
            localLocks[lid].released();
        }

        // Else we add it back to revokeList change its status to free and signal other threads and revoker
        else
        {
            pthread_mutex_lock(&mutexRevokeList);
                revokeList.push_front(lid);
            pthread_mutex_unlock(&mutexRevokeList);
            pthread_cond_signal(&okToRevoke);
            localLocks[lid].unlocked();
        }
    }

    // If we dont have to revoke it, we just release it locally and signal to waiting thread
    else
    {
        localLocks[lid].unlocked();
        rs=lock_protocol::OK;
    }

    return rs;
}


// RPC Procedures
rlock_protocol::status lock_client_cache::retry(lock_protocol::lockid_t lid, int &)
{
    printf("lock_client_cache::retry(%llu)\n", lid);

    // Set retry map value for given lock to true and signal toRetry
    pthread_mutex_lock(&mutexRetryMap);
        retryMap[lid]=true;
    pthread_mutex_unlock(&mutexRetryMap);
    pthread_cond_signal(&okToRetry);
    return rlock_protocol::OK;
}

rlock_protocol::status lock_client_cache::revoke(lock_protocol::lockid_t lid, int &)
{
    printf("lock_client_cache::revoke(%llu)\n", lid);

    // Add given lock to revokeList and signal revoker
    pthread_mutex_lock(&mutexRevokeList);
        revokeList.push_back(lid);
    pthread_mutex_unlock(&mutexRevokeList);
    pthread_cond_signal(&okToRevoke);
    return rlock_protocol::OK;
}


// Implementation of lock_t class

client_lock_t::client_lock_t()
    : lockStatus(NONE)
{
    // initialize condition var
    pthread_cond_init(&okToLock, NULL);

    // initialize mutex
    pthread_mutex_init(&mutex, NULL);
}

int client_lock_t::acquire()
{
    int result;
    // Lock for our lockStatus
    pthread_mutex_lock(&mutex);

    // Wait befor lock is FREE or NONE. In statuses LOCKED, RELEASING or ACQUIRING other thread operates with lock
        while (lockStatus!=FREE && lockStatus!=NONE)
            pthread_cond_wait(&okToLock, &mutex);

        // Setting to result previous value of lockStatus
        result = lockStatus;

        // If previous lockStatus is FREE, we can simply grant it
        if (lockStatus==FREE)
            lockStatus=LOCKED;
        // Else we set it to ACQUIRING and in calling method acquire lock from server
        else
            lockStatus=ACQUIRING;

    // Unlock mutex
    pthread_mutex_unlock(&mutex);
    return result;
}

void client_lock_t::release()
{
    // Just set lock status to RELEASING. It is used always after LOCKED
    pthread_mutex_lock(&mutex);
    assert(lockStatus==LOCKED);
    lockStatus=RELEASING;
    pthread_mutex_unlock(&mutex);
}

void client_lock_t::released()
{
    // After lock is revoked, we set it's status to NONE and unlock next thread waiting for it. It'll acquire it from server
    pthread_mutex_lock(&mutex);
    lockStatus=NONE;
    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&okToLock);
}

void client_lock_t::locked()
{
    // Just set lock status to LOCKED. It is used only after ACQUIRING
    pthread_mutex_lock(&mutex);
    assert(lockStatus==ACQUIRING);
    lockStatus=LOCKED;
    pthread_mutex_unlock(&mutex);
}

void client_lock_t::unlocked()
{
    // After lock is released and not revoked, we set it's status to FREE and unlock next thread waiting for it. It can use it
    pthread_mutex_lock(&mutex);
    lockStatus=FREE;
    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&okToLock);
}

int client_lock_t::status()
{
    pthread_mutex_lock(&mutex);
    int status = lockStatus;
    pthread_mutex_unlock(&mutex);
    return status;
}


