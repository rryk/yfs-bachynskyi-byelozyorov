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
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);

  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::revoke);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);

  // initialize condition var
  pthread_cond_init(&okToRetry, NULL);

  // initialize mutex
  pthread_mutex_init(&mutexRetryMap, NULL);

  // initialize condition var
  pthread_cond_init(&okToRevoke, NULL);

  // initialize mutex
  pthread_mutex_init(&mutexRevokeList, NULL);
}


void
lock_client_cache::releaser()
{


    int r;

    while (true)
    {
        pthread_mutex_lock(&mutexRevokeList);
            while (revokeList.empty())
                    pthread_cond_wait(&okToRevoke, &mutexRevokeList);

            lock_protocol::lockid_t lid=revokeList.front();
            revokeList.pop_front();
        pthread_mutex_unlock(&mutexRevokeList);
            int acqRes=localLocks[lid].acquire();
            assert(acqRes==client_lock_t::FREE || acqRes==client_lock_t::NONE);
            if (acqRes==client_lock_t::FREE)
            {
                localLocks[lid].release();
                lock_protocol::status rs=cl->call(lock_protocol::release, cl->id(), lid, r);
                if (rs==lock_protocol::OK)
                    localLocks[lid].released();
                else
                {
                    pthread_mutex_lock(&mutexRevokeList);
                        revokeList.push_front(lid);
                    pthread_mutex_unlock(&mutexRevokeList);
                        localLocks[lid].unlocked();
                }

            }
            else
                localLocks[lid].released();
    }


}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    if(localLocks[lid].acquire()==client_lock_t::FREE)
        return lock_protocol::OK;
    else
    {
        int r;
        pthread_mutex_lock(&mutexRetryMap);
        retryMap[lid]=false;
        pthread_mutex_unlock(&mutexRetryMap);
        lock_protocol::status as=cl->call(lock_protocol::acquire, cl->id(), lid, id, r);
        assert(as==lock_protocol::OK || as==lock_protocol::RETRY);
        while (as==lock_protocol::RETRY)
        {
            pthread_mutex_lock(&mutexRetryMap);
            while (!retryMap[lid])
                pthread_cond_wait(&okToRetry, &mutexRetryMap);
            retryMap[lid]=false;
            as=cl->call(lock_protocol::acquire, cl->id(), lid, id, r);
            pthread_mutex_unlock(&mutexRetryMap);
        }
        if (as==lock_protocol::OK)
            localLocks[lid].locked();
        else localLocks[lid].released();
        return as;
    }
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    lock_protocol::status rs;
    bool toRevoke=false;
    pthread_mutex_lock(&mutexRevokeList);
    for(std::list<lock_protocol::lockid_t>::iterator it=revokeList.begin();it!=revokeList.end();it++)
        if (*it==lid)
        {
            revokeList.erase(it);
            toRevoke=true;
        }
    pthread_mutex_unlock(&mutexRevokeList);
    if(toRevoke)
    {
        int r;
        localLocks[lid].release();
        rs=cl->call(lock_protocol::release, cl->id(), lid, r);
        if (rs==lock_protocol::OK)
        {
            localLocks[lid].released();
        }
        else
        {
            pthread_mutex_lock(&mutexRevokeList);
            revokeList.push_front(lid);
            pthread_mutex_unlock(&mutexRevokeList);
            localLocks[lid].unlocked();
        }
    }
    else
    {
        localLocks[lid].unlocked();
    }
    return rs;
}

rlock_protocol::status lock_client_cache::retry(lock_protocol::lockid_t lid, int &)
{
    pthread_mutex_lock(&mutexRetryMap);
    retryMap[lid]=true;
    pthread_mutex_unlock(&mutexRetryMap);
    pthread_cond_signal(&okToRetry);
    return rlock_protocol::OK;
}

rlock_protocol::status lock_client_cache::revoke(lock_protocol::lockid_t lid, int &)
{
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
    pthread_mutex_lock(&mutex);
    while (lockStatus==LOCKED || lockStatus==ACQUIRING || lockStatus==RELEASING)
        pthread_cond_wait(&okToLock, &mutex);
    result = lockStatus;
    if (lockStatus==FREE)
        lockStatus=LOCKED;
    else
        lockStatus=ACQUIRING;
    pthread_mutex_unlock(&mutex);
    return result;
}

void client_lock_t::release()
{
    pthread_mutex_lock(&mutex);
    lockStatus=RELEASING;
    pthread_mutex_unlock(&mutex);
}

void client_lock_t::released()
{
    pthread_mutex_lock(&mutex);
    lockStatus=NONE;
    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&okToLock);
}

void client_lock_t::locked()
{
    pthread_mutex_lock(&mutex);
    lockStatus=LOCKED;
    pthread_mutex_unlock(&mutex);
}

void client_lock_t::unlocked()
{
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


