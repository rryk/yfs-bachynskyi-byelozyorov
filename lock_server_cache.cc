// the caching lock server implementation

#include <queue> // causes wierd error with GCC 4.3.2 unless include is placed before header file
                 // this quick-fix is found at http://programming.itags.org/c-c++/180035/

#include "lock_server_cache.h"

#include <sstream>
#include <stdio.h>

#include <unistd.h>
#include <arpa/inet.h>

// rpcc connection cache
std::map<std::string, rpcc*> rpccCache;

// Revoke requested
std::map<lock_protocol::lockid_t, bool> revokeRequested;

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

cache_lock_t::cache_lock_t(lock_protocol::lockid_t lid)
    : id(lid)
    , lockHolder("")
{
    pthread_cond_init(&okToLock, NULL);
    pthread_mutex_init(&mutex, NULL);

    // initialize interested clients list
    interestedClients.clear();

    // set default value for revoke requested status
    revokeRequested[id] = false;
}

lock_protocol::status cache_lock_t::acquire(std::string addr)
{
    printf("lock_t_server::acquire(%s, %llu) ", addr.c_str(), id);

    pthread_mutex_lock(&mutex);
    lock_protocol::status res;

    if (!lockHolder.empty())
    {
        printf("rejected\n");

        // add revoke request to the queue and wake up the revoker thread
        pthread_mutex_lock(&revokeMutex);
        if (!revokeRequested[id])
        {
            revokeRequested[id] = true;
            revokeRequests.push(std::make_pair(lockHolder, id));
        }
        pthread_mutex_unlock(&revokeMutex);
        pthread_cond_signal(&needToRevoke);

        // add client to the list of interested clients
        interestedClients.push_back(addr);
        res = lock_protocol::RETRY;

    }
    else
    {
        printf("granted\n");

        // store current lock holder
        lockHolder = addr;
        res = lock_protocol::OK;

        if (!interestedClients.empty())
        {
            pthread_mutex_lock(&revokeMutex);
            revokeRequested[id] = true;
            revokeRequests.push(std::make_pair(lockHolder, id));
            pthread_mutex_unlock(&revokeMutex);
            pthread_cond_signal(&needToRevoke);
        }
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

    // clear revoke requested status
    revokeRequested[id] = false;

    // notify all interested clients that the lock is available
    pthread_mutex_lock(&retryMutex);
    if (!interestedClients.empty())
    {
        std::string client = interestedClients.front();
        interestedClients.pop_front();
        retryRequests.push(std::make_pair(client, id)); // add repeat request to the queue
        pthread_mutex_unlock(&retryMutex);

        // wake up the retryer thread
        pthread_cond_signal(&needToRetry);
    }
    else
    {
        pthread_mutex_unlock(&retryMutex);
    }

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
        
        // get or create connection to client
        rpcc* cl;
        if (rpccCache.find(req.first) == rpccCache.end())
        {
            sockaddr_in dstsock;
            make_sockaddr(req.first.c_str(), &dstsock);
            cl = new rpcc(dstsock);

            if (cl->bind() < 0) {
                printf("lock_server_cache::revoker(): bind failed\n");
                return;
            }

            rpccCache.insert(std::make_pair(req.first, cl));
        }
        else
        {
            cl = rpccCache[req.first];
        }

        // send revoke request
        printf("lock_server_cache::send_revoke(%s, %llu)\n", req.first.c_str(), req.second);
        if (cl->call(rlock_protocol::revoke, req.second, r) != rlock_protocol::OK)
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

        // get or create connection to client
        rpcc* cl;
        if (rpccCache.find(req.first) == rpccCache.end())
        {
            sockaddr_in dstsock;
            make_sockaddr(req.first.c_str(), &dstsock);
            cl = new rpcc(dstsock);

            if (cl->bind() < 0) {
                printf("lock_server_cache::revoker(): bind failed\n");
                return;
            }

            rpccCache.insert(std::make_pair(req.first, cl));
        }
        else
        {
            cl = rpccCache[req.first];
        }

        // send revoke request
        printf("lock_server_cache::send_retry(%s, %llu)\n", req.first.c_str(), req.second);
        if (cl->call(rlock_protocol::retry, req.second, r) != rlock_protocol::OK)
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
    printf("lock_server_cache::acquire(%d, %s, %llu)\n", clt, rpc_addr.c_str(), lid);

    // insert new lock record on first access (for unknown-before lock id)
    pthread_mutex_lock(&mutex);
    if (locks.find(lid) == locks.end())
        locks.insert(std::make_pair(lid, cache_lock_t(lid)));
    pthread_mutex_unlock(&mutex);

    r = locks[lid].acquire(rpc_addr);

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

    return lock_protocol::OK;
}
