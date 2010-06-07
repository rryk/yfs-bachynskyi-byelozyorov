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
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);
}


void
lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.


}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
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

void client_lock_t::acquired()
{
    pthread_mutex_lock(&mutex);
    while (lockStatus!=FREE)
        pthread_cond_wait(&okToLock, &mutex);
    lockStatus=LOCKED;
    pthread_mutex_unlock(&mutex);
}

void client_lock_t::revoked()
{
    pthread_mutex_lock(&mutex);
    lockStatus=FREE;
    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&okToLock);
}

void client_lock_t::acquiring()
{
    pthread_mutex_lock(&mutex);
    lockStatus=ACQUIRING;
    pthread_mutex_unlock(&mutex);
}

void client_lock_t::releasing()
{
    pthread_mutex_lock(&mutex);
    lockStatus=RELEASING;
    pthread_mutex_unlock(&mutex);
}

int client_lock_t::status()
{
    pthread_mutex_lock(&mutex);
    int status = lockStatus;
    pthread_mutex_unlock(&mutex);
    return status;
}


