// the caching lock server implementation

#include <queue> // causes wierd error with GCC 4.3.2 unless include is placed before header file
                 // this quick-fix is found at http://programming.itags.org/c-c++/180035/

#include "lock_server_cache.h"

#include <sstream>
#include <stdio.h>

#include <unistd.h>
#include <arpa/inet.h>

// Revoke and retry request queues
std::queue<std::pair<std::string, lock_protocol::lockid_t> > revokeRequests;
std::queue<std::pair<std::string, lock_protocol::lockid_t> > retryRequests;

// Mutexes to protect revoke and retry request queues
pthread_mutex_t revokeMutex;
pthread_mutex_t retryMutex;

// Condition variable to allow waking up revoke and retry request handler processes
pthread_cond_t needToRevoke;
pthread_cond_t needToRetry;

// Implementation of lock_server_cache class

cache_lock_t::cache_lock_t(int lid)
    : id(lid)
    , lockHolder("")
{
    pthread_cond_init(&okToLock, NULL);
    pthread_mutex_init(&mutex, NULL);

    // initialize interested clients list
    interestedClients.clear();
}

lock_protocol::status cache_lock_t::acquire(std::string addr)
{
	printf("lock_t_server::acquire(%llu)\n", id);

    pthread_mutex_lock(&mutex);
    lock_protocol::status res;

    if (!lockHolder.empty())
    {
        // add revoke request to the queue and wake up the revoker thread
        pthread_mutex_lock(&revokeMutex);
        revokeRequests.push(std::make_pair(lockHolder, id));
        pthread_mutex_unlock(&revokeMutex);
        pthread_cond_signal(&needToRevoke);

        // add client to the list of interested clients
        interestedClients.push_back(addr);
        res = lock_protocol::RETRY;
    }
    else
    {
        // store current lock holder
        lockHolder = addr;
        res = lock_protocol::OK;
    }

    pthread_mutex_unlock(&mutex);
    return res;
}

void cache_lock_t::release()
{
    pthread_mutex_lock(&mutex);

    assert(!lockHolder.empty());

    // clear lock holder
    lockHolder = "";

    // notify all interested clients that the lock is available
    pthread_mutex_lock(&retryMutex);
    for (std::list<std::string>::const_iterator it = interestedClients.begin(); it != interestedClients.end(); it++)
        retryRequests.push(std::make_pair(*it, id)); // add repeat request to the queue
    pthread_mutex_unlock(&retryMutex);

    // wake up the retryer thread
    pthread_cond_signal(&needToRetry);

    // erase list of interested clients (one of them will acquire the lock and others will register again)
    interestedClients.clear();

    pthread_mutex_unlock(&mutex);
}

bool cache_lock_t::isLocked()
{
    // make temporary variable and return it
    pthread_mutex_lock(&mutex);
    bool isLocked = !lockHolder.empty();
    pthread_mutex_unlock(&mutex);
    return isLocked;
}

// Implementation of lock_server_cache class

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
    // initialize lock map access mutex
    pthread_mutex_init(&mutex, NULL);

    pthread_t th;
    int r = pthread_create(&th, NULL, &revokethread, (void *) this);
    assert (r == 0);
    r = pthread_create(&th, NULL, &retrythread, (void *) this);
    assert (r == 0);
}

void
lock_server_cache::revoker()
{
    // initialize condition var and mutex
    pthread_cond_init(&needToRevoke, NULL);
    pthread_mutex_init(&revokeMutex, NULL);

    // temp variable to store RPC result
    int r;

    while (true)
    {
        pthread_mutex_lock(&revokeMutex);

        // wait for new revoke requests
        while (revokeRequests.empty())
            pthread_cond_wait(&needToRevoke, &revokeMutex);

        // get request details
        std::pair<std::string, lock_protocol::lockid_t> req =
        revokeRequests.front();

        // remove processed request
        revokeRequests.pop();

        pthread_mutex_unlock(&revokeMutex);

        // send revoke request
        sockaddr_in dstsock;
        make_sockaddr(req.first.c_str(), &dstsock);
        rpcc cl(dstsock);
        if (cl.call(rlock_protocol::revoke, req.second, r)!=lock_protocol::OK)
        {
            pthread_mutex_lock(&revokeMutex);
            revokeRequests.push(req);
            pthread_mutex_unlock(&revokeMutex);
        }
    }
}


void
lock_server_cache::retryer()
{
    // initialize condition var and mutex
    pthread_cond_init(&needToRetry, NULL);
    pthread_mutex_init(&retryMutex, NULL);

    // temp variable to store RPC result
    int r;

    while (true)
    {
        printf("lock_server_cache::retryer\n");
        pthread_mutex_lock(&retryMutex);

        // wait for new revoke requests
        while (retryRequests.empty())
            pthread_cond_wait(&needToRetry, &retryMutex);

        // get request details
        std::pair<std::string, lock_protocol::lockid_t> req =
            retryRequests.front();

        // remove processed request
        retryRequests.pop();

        pthread_mutex_unlock(&retryMutex);

        // send retry request
        sockaddr_in dstsock;
        make_sockaddr(req.first.c_str(), &dstsock);
        rpcc cl(dstsock);
        if (cl.call(rlock_protocol::retry, req.second, r)!=lock_protocol::OK)
        {
            pthread_mutex_lock(&retryMutex);
            retryRequests.push(req);
            pthread_mutex_unlock(&retryMutex);
        }
    }
}

lock_protocol::status lock_server_cache::stat(int clt, lock_protocol::lockid_t lid, int & r)
{
    printf("lock_server_cache::stat(%d, %llu)\n", clt, lid);

    // insert new lock record on first access (for unknown-before lock id)
    pthread_mutex_lock(&mutex);
    if (locks.find(lid) == locks.end())
        locks.insert(std::make_pair(lid, cache_lock_t(lid)));
    pthread_mutex_unlock(&mutex);

    r = locks[lid].isLocked() ? 1 : 0;

    return lock_protocol::OK;
}

lock_protocol::status lock_server_cache::acquire(int clt, lock_protocol::lockid_t lid, std::string rpc_addr, lock_protocol::status & r)
{
    printf("lock_server_cache::acquire(%d, %llu)\n", clt, lid);

    // insert new lock record on first access (for unknown-before lock id)
    pthread_mutex_lock(&mutex);
    if (locks.find(lid) == locks.end())
        locks.insert(std::make_pair(lid, cache_lock_t(lid)));
    pthread_mutex_unlock(&mutex);
	printf("lock_server_cache::acquire(%d, %llu) Middle\n", clt, lid);

    r = locks[lid].acquire(rpc_addr);
    printf("lock_server_cache::acquire(%d, %llu) End\n", clt, lid);

    return r;
}

lock_protocol::status lock_server_cache::release(int clt, lock_protocol::lockid_t lid, int &)
{
    printf("lock_server_cache::release(%d, %llu)\n", clt, lid);

    // insert new lock record on first access (for unknown-before lock id)
    pthread_mutex_lock(&mutex);
    if (locks.find(lid) == locks.end())
        return lock_protocol::NOENT;
    pthread_mutex_unlock(&mutex);

    locks[lid].release();
    printf("lock_server_cache::release(%d, %llu) End\n", clt, lid);

    return lock_protocol::OK;
}
