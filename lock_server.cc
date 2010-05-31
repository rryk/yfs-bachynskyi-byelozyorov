// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

// Implementation of lock_t class

lock_t::lock_t()
    : locked(false)
{
    // initialize condition var
    pthread_cond_init(&okToLock, NULL);
    
    // initialize mutex
    pthread_mutex_init(&mutex, NULL);
}

void lock_t::acquire()
{
    pthread_mutex_lock(&mutex);

    // wait until lock releases
    while (locked)
        pthread_cond_wait(&okToLock, &mutex);

    // lock it again
    locked = true;

    pthread_mutex_unlock(&mutex);
}

void lock_t::release()
{
    pthread_mutex_lock(&mutex);

    // release current lock
    locked = false;

    pthread_mutex_unlock(&mutex);

    // notify one of the waiting threads
    pthread_cond_signal(&okToLock);
}

bool lock_t::isLocked()
{
    pthread_mutex_lock(&mutex);

    // copy lock status into a local var
    bool isLocked = locked;

    pthread_mutex_unlock(&mutex);

    // return lock status
    return isLocked;
}

// Implementation of lock_server class

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
    printf("server: requested status of lock %llu\n", lid);

    // locks[lid] is created automatically when accessing first time
    // return should be 1 (locked) or 0 (unlocked)
    r = locks[lid].isLocked() ? 1 : 0;

    return lock_protocol::OK;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &)
{
    printf("server: acquired lock %llu\n", lid);

    // locks[lid] is created automatically when accessing first time
    locks[lid].acquire();

    return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &)
{
    printf("server: released lock %llu\n", lid);

    // locks[lid] is created automatically when accessing first time
    locks[lid].release();

    return lock_protocol::OK;
}


