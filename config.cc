#include <sstream>
#include <iostream>
#include <stdio.h>
#include "config.h"
#include "paxos.h"
#include "handle.h"

// The config module maintains views. As a node joins or leaves a
// view, the next view will be the same as previous view, except with
// the new node added or removed. The first view contains only node
// 1. If node 2 joins after the first node (it will download the views
// from node 1), it will learn about view 1 with the first node as the
// only member.  It will then invoke Paxos to create the next view.
// It will tell Paxos to ask the nodes in view 1 to agree on the value
// {1, 2}.  If Paxos returns success, then it moves to view 2 with
// {1,2} as the members. When node 3 joins, the config module runs
// Paxos with the nodes in view 2 and the proposed value to be
// {1,2,3}. And so on.  When a node discovers that some node of the
// current view is not responding, it kicks off Paxos to propose a new
// value (the current view minus the node that isn't responding). The
// config module uses Paxos to create a total order of views, and it
// is ensured that the majority of the previous view agrees to the
// next view.  The Paxos log contains all the values (i.e., views)
// agreed on.
//
// The RSM module informs config to add nodes. The config module
// runs a heartbeater thread that checks in with nodes.  If a node
// doesn't respond, the config module will invoke Paxos's proposer to
// remove the node.  Higher layers will learn about this change when a
// Paxos acceptor accepts the new proposed value through
// paxos_commit().
//
// To be able to bring other nodes up to date to the latest formed
// view, each node will have a complete history of all view numbers
// and their values that it knows about. At any time a node can reboot
// and when it re-joins, it may be many views behind; by remembering
// all views, the other nodes can bring this re-joined node up to
// date.

static void *
heartbeatthread(void *x)
{
  config *r = (config *) x;
  r->heartbeater();
  return 0;
}

config::config(std::string _first, std::string _me, config_view_change *_vc) 
  : myvid (0), first (_first), me (_me), vc (_vc)
{
  assert (pthread_mutex_init(&cfg_mutex, NULL) == 0);
  assert(pthread_cond_init(&config_cond, NULL) == 0);  

  std::ostringstream ost;
  ost << me;

  acc = new acceptor(this, me == _first, me, ost.str());
  pro = new proposer(this, acc, me);

  // XXX hack; maybe should have its own port number
  pxsrpc = acc->get_rpcs();
  pxsrpc->reg(paxos_protocol::heartbeat, this, &config::heartbeat);

  assert(pthread_mutex_lock(&cfg_mutex)==0);

  reconstruct();

  pthread_t th;
  assert (pthread_create(&th, NULL, &heartbeatthread, (void *) this) == 0);
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
}

void
config::restore(std::string s)
{
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  acc->restore(s);
  reconstruct();
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
}

// caller should hold cfg_mutex
std::vector<std::string>
config::get_view(unsigned instance)
{
  std::string value = acc->value(instance);
  printf("get_view(%d): returns %s\n", instance, value.c_str());
  return members(value);
}

std::vector<std::string>
config::members(std::string value)
{
  std::istringstream ist(value);
  std::string m;
  std::vector<std::string> view;
  while (ist >> m) {
    view.push_back(m);
  }
  return view;
}

std::string
config::value(std::vector<std::string> m)
{
  std::ostringstream ost;
  for (unsigned i = 0; i < m.size(); i++)  {
    ost << m[i];
    ost << " ";
  }
  return ost.str();
}

// caller should hold cfg_mutex
void
config::reconstruct()
{
  if (acc->instance() > 0) {
    std::string m;
    myvid = acc->instance();
    mems = get_view(myvid);
    printf("config::reconstruct: %d %s\n", myvid, print_members(mems).c_str());
  }
}

// Called by Paxos's acceptor.
void
config::paxos_commit(unsigned instance, std::string value)
{
  std::string m;
  std::vector<std::string> newmem;
  assert(pthread_mutex_lock(&cfg_mutex)==0);

  newmem = members(value);
  printf("config::paxos_commit: %d: %s\n", instance, 
	 print_members(newmem).c_str());

  for (unsigned i = 0; i < mems.size(); i++) {
    printf("config::paxos_commit: is %s still a member?\n", mems[i].c_str());
    if (!isamember(mems[i], newmem) && me != mems[i]) {
      printf("config::paxos_commit: delete %s\n", mems[i].c_str());
      mgr.delete_handle(mems[i]);
    }
  }

  mems = newmem;
  myvid = instance;
  if (vc) {
    assert(pthread_mutex_unlock(&cfg_mutex)==0);
    vc->commit_change();
    assert(pthread_mutex_lock(&cfg_mutex)==0);
  }
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
}

bool
config::ismember(std::string m)
{
  bool r;
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  r = isamember(m, mems);
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  return r;
}

std::vector<std::string> 
config::get_curview()
{ 
  std::vector<std::string> v;
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  v = get_view(myvid);
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  return v;
}

std::vector<std::string> 
config::get_prevview()
{ 
  std::vector<std::string> v;
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  v = get_view(myvid - 1);
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  return v;
}

std::string 
config::print_curview() 
{ 
  std::string s;
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  s = print_members(mems); 
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  return s;
};

bool
config::add(std::string new_m)
{
  std::vector<std::string> m;
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  printf("config::add %s\n", new_m.c_str());
  m = mems;
  m.push_back(new_m);
  std::string v = value(m);
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  bool r = pro->run(myvid+1, mems, v);
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  if (r) {
    printf("config::add: proposer returned success\n");
  } else {
    printf("config::add: proposer returned failure\n");
  }
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  return r;
}

// caller should hold cfg_mutex
bool
config::remove_wo(std::string m)
{
  printf("config::remove: myvid %d remove? %s\n", myvid, m.c_str());
  std::vector<std::string> n;
  for (unsigned i = 0; i < mems.size(); i++) {
    if (mems[i] != m) n.push_back(mems[i]);
  }
  std::string v = value(n);
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  bool r = pro->run(myvid+1, mems, v);
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  if (r) {
    printf("config::remove: proposer returned success\n");
  } else {
    printf("config::remove: proposer returned failure\n");
  }
  return r;
}

void
config::heartbeater()
{
  struct timeval now;
  struct timespec next_timeout;
  std::string m;
  heartbeat_t h;
  bool stable;

  assert(pthread_mutex_lock(&cfg_mutex)==0);
  
  while (1) {

    gettimeofday(&now, NULL);
    next_timeout.tv_sec = now.tv_sec + 3;
    next_timeout.tv_nsec = 0;
    printf("heartbeater: go to sleep\n");
    pthread_cond_timedwait(&config_cond, &cfg_mutex, &next_timeout);

    stable = true;

    printf("heartbeater: current membership %s\n", print_members(mems).c_str());

    if (!isamember(me, mems)) {
      printf("heartbeater: not member yet; skip hearbeat\n");
      continue;
    }

    //find the node with the smallest id
    m = me;
    for (unsigned i = 0; i < mems.size(); i++) {
      if (m > mems[i])
	m = mems[i];
    }

    if (m == me) {
      //if i am the one with smallest id, ping the rest of the nodes
      for (unsigned i = 0; i < mems.size(); i++) {
	if (mems[i] != me) {
	  if ((h = doheartbeat(mems[i])) != OK) {
	    stable = false;
	    m = mems[i];
	    break;
	  }
	}
      }
    } else {
      //the rest of the nodes ping the one with smallest id
	if ((h = doheartbeat(m)) != OK) 
	    stable = false;
    }

    if (!stable) {
      remove_wo(m);
    }
  }

  assert(pthread_mutex_unlock(&cfg_mutex)==0);
}

paxos_protocol::status
config::heartbeat(std::string m, unsigned vid, int &r)
{
  assert(pthread_mutex_lock(&cfg_mutex)==0);
  int ret = paxos_protocol::ERR;
  r = (int) myvid;
  printf("heartbeat from %s(%d) myvid %d\n", m.c_str(), vid, myvid);
  if (vid == myvid) {
    ret = paxos_protocol::OK;
  } else if (pro->isrunning()) {
    assert (vid == myvid + 1 || vid + 1 == myvid);
    ret = paxos_protocol::OK;
  } else {
    ret = paxos_protocol::ERR;
  }
  assert(pthread_mutex_unlock(&cfg_mutex)==0);
  return ret;
}

config::heartbeat_t
config::doheartbeat(std::string m)
{
  int ret = rpc_const::timeout_failure;
  int r;
  unsigned vid = myvid;
  heartbeat_t res = OK;

  printf("doheartbeater to %s (%d)\n", m.c_str(), vid);
  handle h(m);
  if (h.get_rpcc()) {
    assert(pthread_mutex_unlock(&cfg_mutex)==0);
    ret = h.get_rpcc()->call(paxos_protocol::heartbeat, me, vid, r, 
			 rpcc::to(1000));
    assert(pthread_mutex_lock(&cfg_mutex)==0);
  } 
  if (ret != paxos_protocol::OK) {
    if (ret == rpc_const::atmostonce_failure || 
	ret == rpc_const::oldsrv_failure) {
      mgr.delete_handle(m);
    } else {
      printf("doheartbeat: problem with %s (%d) my vid %d his vid %d\n", 
	     m.c_str(), ret, vid, r);
      if (ret < 0) res = FAILURE;
      else res = VIEWERR;
    }
  }
  printf("doheartbeat done %d\n", res);
  return res;
}

