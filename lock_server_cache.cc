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
std::list<std::pair<std::string, lock_protocol::lockid_t> > revokeRequests;
std::list<std::pair<std::string, lock_protocol::lockid_t> > retryRequests;
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
            revokeRequests.push_back(std::make_pair(lockHolder, id));
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
            revokeRequests.push_back(std::make_pair(lockHolder, id));
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
        retryRequests.push_back(std::make_pair(client, id)); // add repeat request to the queue
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

lock_server_cache::lock_server_cache(class rsm *_rsm) 
  : rsm (_rsm)
{
    // initialize lock map access mutex
    pthread_mutex_init(&mutex, NULL);

    rsm->set_state_transfer(this);
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
        printf("revoker: torevoke");
        if(rsm->amiprimary())
        {
            pthread_mutex_lock(&revokeMutex);

            // wait for new revoke requests
            while (revokeRequests.empty())
                pthread_cond_wait(&needToRevoke, &revokeMutex);

            // get request details
            std::pair<std::string, lock_protocol::lockid_t> req =
                revokeRequests.front();

            // remove processed request
            revokeRequests.pop_front();

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
                revokeRequests.push_back(req);
                pthread_mutex_unlock(&revokeMutex);
            }
        }
        else
        {
            sleep(5);
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
        printf("retryer: toretry");

        if (rsm->amiprimary())
        {
            pthread_mutex_lock(&retryMutex);
            // wait for new revoke requests
            while (retryRequests.empty())
                pthread_cond_wait(&needToRetry, &retryMutex);

            // get request details
            std::pair<std::string, lock_protocol::lockid_t> req =
                retryRequests.front();

            // remove processed request
            retryRequests.pop_front();

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
                retryRequests.push_back(req);
                pthread_mutex_unlock(&retryMutex);
            }
        }
        else
        {
            sleep(5);
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

lock_protocol::status lock_server_cache::acquire(int clt, long long unsigned int reqID, lock_protocol::lockid_t lid, std::string rpc_addr, lock_protocol::status & r)
{
    printf("lock_server_cache::acquire(%d, %s, %llu)\n", clt, rpc_addr.c_str(), lid);
    if (! rsm->amiprimary_wo())
    {
        printf("Not primary acquire\n");
        // Delete appropriate request from retry queue
        pthread_mutex_lock(&retryMutex);
        if(!retryRequests.empty())
        {
            for(std::list<std::pair<std::string, lock_protocol::lockid_t> >::iterator it=retryRequests.begin();it!=retryRequests.end();it++)
            {
                std::pair<std::string, lock_protocol::lockid_t> pair=*it;
                if(pair.first.compare(rpc_addr)==0 && pair.second==lid)
                {
                    retryRequests.erase(it);
                    break;
                }
            }
        }
        pthread_mutex_unlock(&retryMutex);
    }

    pthread_mutex_lock(&mutex);
    if(rpcDone.find(clt)!=rpcDone.end())
    {
        if(rpcDone[clt].find(reqID)!=rpcDone[clt].end())
        {
            lock_protocol::status res=rpcDone[clt][reqID];
            pthread_mutex_unlock(&mutex);
            return res;
        }
    }


    // insert new lock record on first access (for unknown-before lock id)
    if (locks.find(lid) == locks.end())
        locks.insert(std::make_pair(lid, cache_lock_t(lid)));

    rpcDone[clt][reqID]=r;
    pthread_mutex_unlock(&mutex);

    r = locks[lid].acquire(rpc_addr);

    return r;
}

lock_protocol::status lock_server_cache::release(int clt, long long unsigned int reqID, lock_protocol::lockid_t lid, int &)
{
    printf("lock_server_cache::release(%d, %llu)\n", clt, lid);


    if (! rsm->amiprimary_wo())
    {
        printf("Not primary release\n");
        // Delete appropriate request from retry queue
        pthread_mutex_lock(&revokeMutex);
        if(!revokeRequests.empty())
        {
            for(std::list<std::pair<std::string, lock_protocol::lockid_t> >::iterator it=revokeRequests.begin();it!=revokeRequests.end();it++)
            {
                std::pair<std::string, lock_protocol::lockid_t> pair=*it;
                if(pair.second==lid)
                {
                    revokeRequests.erase(it);
                    break;
                }
            }
        }
        pthread_mutex_unlock(&revokeMutex);
    }

    pthread_mutex_lock(&mutex);
    if(rpcDone.find(clt)!=rpcDone.end())
    {
        if(rpcDone[clt].find(reqID)!=rpcDone[clt].end())
        {
            lock_protocol::status res=rpcDone[clt][reqID];
            pthread_mutex_unlock(&mutex);
            return res;
        }
    }

    // insert new lock record on first access (for unknown-before lock id)
    if (locks.find(lid) == locks.end())
    {
        rpcDone[clt][reqID]=lock_protocol::NOENT;
        pthread_mutex_unlock(&mutex);
        return lock_protocol::NOENT;
    }

    rpcDone[clt][reqID]=lock_protocol::OK;
    pthread_mutex_unlock(&mutex);

    locks[lid].release();

    return lock_protocol::OK;
}

unmarshall &
operator>>(unmarshall &u, struct cache_lock_t &s)
{
    pthread_mutex_lock(&s.mutex);
    u >> s.id;
    u >> s.lockHolder;
    unsigned int listSize;
    u >> listSize;
    for(int j = 0; j < listSize; j++)
    {
        std::string i;
        u >> i;
        s.interestedClients.push_back(i);
    }
    pthread_mutex_unlock(&s.mutex);
    return u;
}

marshall &
operator<<(marshall &m, struct cache_lock_t &s)
{
    pthread_mutex_lock(&s.mutex);
    m << s.id;
    m << s.lockHolder;
    m << s.interestedClients.size();
    for(std::list<std::string>::iterator it=s.interestedClients.begin(); it!=s.interestedClients.end(); it++)
        m << (*it);
    pthread_mutex_unlock(&s.mutex);
    return m;
}


std::string lock_server_cache::marshal_state() {
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&revokeMutex);
    pthread_mutex_lock(&retryMutex);
        marshall rep;

        rep << locks.size();
        std::map<lock_protocol::lockid_t, cache_lock_t>::iterator iter_lock;
        for (iter_lock = locks.begin(); iter_lock != locks.end(); iter_lock++) {
            lock_protocol::lockid_t id=iter_lock->first;
            rep << id;
            rep << locks[id];
        }

        rep<<revokeRequests.size();
        for(std::list<std::pair<std::string, lock_protocol::lockid_t> >::iterator it=revokeRequests.begin(); it!=revokeRequests.end(); it++)
        {
            rep << it->first;
            rep << it->second;
        }
        rep<<retryRequests.size();

        for(std::list<std::pair<std::string, lock_protocol::lockid_t> >::iterator it=retryRequests.begin(); it!=retryRequests.end(); it++)
        {
            rep << it->first;
            rep << it->second;
        }

        rep << rpcDone.size();
        std::map<int, std::map<long long unsigned int, lock_protocol::status> >::iterator oldRPCit;
        for (oldRPCit = rpcDone.begin(); oldRPCit != rpcDone.end(); oldRPCit++) {
            int clt=oldRPCit->first;
            rep << clt;
            std::map<long long unsigned int, lock_protocol::status> requests=oldRPCit->second;

            rep << requests.size();
            std::map<long long unsigned int, lock_protocol::status>::iterator it;
            for (it = requests.begin(); it != requests.end(); it++) {
                long long unsigned int reqID=it->first;
                rep << reqID;
                lock_protocol::status stat=it->second;
                rep << stat;
            }
        }

    pthread_mutex_lock(&mutex);
    pthread_mutex_unlock(&revokeMutex);
    pthread_mutex_unlock(&retryMutex);

    return rep.str();
}



void lock_server_cache::unmarshal_state(std::string state) {
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&revokeMutex);
    pthread_mutex_lock(&retryMutex);
        unmarshall rep(state);
        unsigned int size;
        rep >> size;
        for (unsigned int i = 0; i < size; i++)
        {
            lock_protocol::lockid_t id;
            rep >> id;
            cache_lock_t lock(id);
            rep >> lock;
            locks[id] = lock;
        }

        rep>>size;
        for (unsigned int i = 0; i < size; i++)
        {
            std::string first;
            lock_protocol::lockid_t second;
            rep >> first;
            rep >> second;
            revokeRequests.push_back(std::make_pair(first,second));
        }

        rep>>size;
        for (unsigned int i = 0; i < size; i++)
        {
            std::string first;
            lock_protocol::lockid_t second;
            rep >> first;
            rep >> second;
            retryRequests.push_back(std::make_pair(first,second));
        }

        rep >> size;
        for (unsigned int i = 0; i < size; i++) {
            int clt;
            rep >> clt;
            std::map<long long unsigned int, lock_protocol::status> requests;

            unsigned int size2;
            rep >> size2;
            for (unsigned int j = 0; j < size2; j++) {
                long long unsigned int reqID;
                rep >> reqID;
                lock_protocol::status stat;
                rep >> stat;
                requests[reqID]=stat;
            }
            rpcDone[clt]=requests;
        }



    pthread_mutex_lock(&mutex);
    pthread_mutex_unlock(&revokeMutex);
    pthread_mutex_unlock(&retryMutex);
}
