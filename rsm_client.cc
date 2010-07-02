#include "rsm_client.h"
#include <vector>
#include <arpa/inet.h>
#include <stdio.h>


rsm_client::rsm_client(std::string dst)
{
  printf("create rsm_client\n");
  std::vector<std::string> mems;

  pthread_mutex_init(&rsm_client_mutex, NULL);
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  primary.id = dst;
  primary.cl = new rpcc(dstsock);
  primary.nref = 0;
  int ret = primary.cl->bind(rpcc::to(1000));
  if (ret < 0) {
    printf("rsm_client::rsm_client bind failure %d failure w %s; exit\n", ret, 
     primary.id.c_str());
    exit(1);
  }
  assert(pthread_mutex_lock(&rsm_client_mutex)==0);
  assert (init_members(true));
  assert(pthread_mutex_unlock(&rsm_client_mutex)==0);
  printf("rsm_client: done\n");
}

// Assumes caller holds rsm_client_mutex 
void
rsm_client::primary_failure()
{
  // For lab 8
}

rsm_protocol::status
rsm_client::invoke(int proc, std::string req, std::string &rep)
{
  int ret;
  rpcc *cl;
  assert(pthread_mutex_lock(&rsm_client_mutex)==0);
  while (1) {
    printf("rsm_client::invoke proc %x primary %s\n", proc, primary.id.c_str());
    cl = primary.cl;
    primary.nref++;
    assert(pthread_mutex_unlock(&rsm_client_mutex)==0);
    ret = primary.cl->call(rsm_client_protocol::invoke, proc, req, 
        rep, rpcc::to(5000));
    assert(pthread_mutex_lock(&rsm_client_mutex)==0);
    primary.nref--;
    printf("rsm_client::invoke proc %x primary %s ret %d\n", proc, 
     primary.id.c_str(), ret);
    if (ret == rsm_client_protocol::OK) {
      break;
    }
    if (ret == rsm_client_protocol::BUSY) {
      printf("rsm is busy %s\n", primary.id.c_str());
      sleep(3);
      continue;
    }
    if (ret == rsm_client_protocol::NOTPRIMARY) {
      printf("primary %s isn't the primary--let's get a complete list of mems\n", 
          primary.id.c_str());
      if (init_members(true))
        continue;
    }
    printf("primary %s failed ret %d\n", primary.id.c_str(), ret);
    primary_failure();
    printf ("rsm_client::invoke: retry new primary %s\n", primary.id.c_str());
  }
  assert(pthread_mutex_unlock(&rsm_client_mutex)==0);
  return ret;
}

bool
rsm_client::init_members(bool send_member_rpc)
{
  if (send_member_rpc) {
    printf("rsm_client::init_members get members!\n");
    assert(pthread_mutex_unlock(&rsm_client_mutex)==0);
    int ret = primary.cl->call(rsm_client_protocol::members, 0, known_mems, 
            rpcc::to(1000)); 
    assert(pthread_mutex_lock(&rsm_client_mutex)==0);
    if (ret != rsm_protocol::OK)
      return false;
  }
  if (known_mems.size() < 1) {
    printf("rsm_client::init_members do not know any members!\n");
    assert(0);
  }

  std::string new_primary = known_mems.back();
  known_mems.pop_back();

  printf("rsm_client::init_members: primary %s\n", new_primary.c_str());

  if (new_primary != primary.id) {
    sockaddr_in dstsock;
    make_sockaddr(new_primary.c_str(), &dstsock);
    primary.id = new_primary;
    if (primary.cl) {
      assert(primary.nref == 0);  // XXX fix: delete cl only when refcnt=0
      delete primary.cl; 
    }
    primary.cl = new rpcc(dstsock);

    if (primary.cl->bind(rpcc::to(1000)) < 0) {
      printf("rsm_client::rsm_client cannot bind to primary\n");
      return false;
    }
  }
  return true;
}

