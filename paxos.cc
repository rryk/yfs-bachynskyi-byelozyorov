#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  assert(pthread_mutex_lock(&pxs_mutex)==0);
  r = !stable;
  assert(pthread_mutex_unlock(&pxs_mutex)==0);
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

}

bool
proposer::run(int instance, std::vector<std::string> nodes, std::string newv)
{
    std::vector<std::string> accepts;
    std::vector<std::string> nodes1;
    std::string v;
    bool r = false;

    pthread_mutex_lock(&pxs_mutex);
    printf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
        print_members(nodes).c_str(), instance, newv.c_str(), stable);
    if (!stable) {  // already running proposer?
        printf("proposer::run: already running\n");
        pthread_mutex_unlock(&pxs_mutex);
        return false;
    }

    stable = false;
    c_nodes = nodes;
    c_v = newv;

    accepts.clear();
    v.clear();
    if (prepare(instance, accepts, nodes, v))
    {
        if (majority(nodes, accepts))
        {
            printf("paxos::manager: received a majority of prepare responses\n");

            if (v.size() == 0) {
                v = c_v;
            }

            breakpoint1();

            nodes1 = accepts;
            accepts.clear();
            accept(instance, accepts, nodes1, v); // FIXME: add support for oldinstance from accept RPC

            if (majority(c_nodes, accepts)) {
                printf("paxos::manager: received a majority of accept responses, v=%s\n", v.c_str());

                breakpoint2();

                decide(instance, accepts, v); // FIXME: add support for oldinstance from decide RPC
                r = true;
            }
            else
            {
                // TODO: handle the case when only minority of the nodes responds to the prepare
                printf("paxos::manager: no majority of accept responses\n");
            }
        }
        else
        {
            // TODO: handle the case when only minority of the nodes responds to the prepare
            printf("paxos::manager: no majority of prepare responses\n");
        }
    }
    else
    {
        // TODO: handle reject response by one of the clients
        printf("paxos::manager: prepare is rejected %d\n", stable);
    }

    stable = true;
    pthread_mutex_unlock(&pxs_mutex);
    return r;
}

bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
    printf("proposer:prepare: calculating my_n, before, my_n.n=%u, my_n.m=%s, acc->get_n_h().n=%u, me=%s\n", my_n.n, my_n.m.c_str(), acc->get_n_h().n, me.c_str());
    my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
    my_n.m = me;
    printf("proposer:prepare: calculating my_n, after, my_n.n=%u, my_n.m=%s, acc->get_n_h().n=%u, me=%s\n", my_n.n, my_n.m.c_str(), acc->get_n_h().n, me.c_str());

    // set maximum id to the minimum (to be updated in a loop with larger id)
    prop_t max_n_a = {0, std::string()};

    for (unsigned i = 0; i < nodes.size(); i++)
    {
        handle h(nodes[i]);

        if (h.get_rpcc()) {
            paxos_protocol::prepareres res;
            paxos_protocol::preparearg arg;

            arg.instance = instance;
            arg.n = my_n;

            printf("proposer::prepare: sending preparereq RPC to %s\n", nodes[i].c_str());

            if (h.get_rpcc()->call(paxos_protocol::preparereq, me, arg, res, rpcc::to(1000)) == paxos_protocol::OK)
            {
                if (res.oldinstance)
                {
                    printf("proposer::prepare: got oldinstance from %s\n", nodes[i].c_str());
                    acc->commit(instance, res.v_a);
                    stable = true;

                    return false;
                }
                else if (res.accept)
                {
                    printf("proposer::prepare: got prepareres from %s, res.n_a.n=%u, res.n_a.m=%s, res.v_a=%s\n", nodes[i].c_str(), res.n_a.n, res.n_a.m.c_str(), res.v_a.c_str());

                    // add node to the list of accepted nodes
                    accepts.push_back(nodes[i]);

                    // check if the node returned value and it's ID it largest than last found
                    if (res.v_a.size() != 0 && res.n_a > max_n_a)
                    {
                        max_n_a = res.n_a;
                        v = res.v_a;
                        printf("proposer::propose: updated value to newv=%s, max_n_a.n=%u, max_n_a.m=%s\n",
                               v.c_str(), max_n_a.n, max_n_a.m.c_str());
                    }
                }
                else
                {
                    printf("proposer::prepare: got reject from %s\n", nodes[i].c_str());
                    return false;
                }
            }
            else
            {
                printf("proposer::prepare: failed to get response from %s\n", nodes[i].c_str());
            }
        }
        else
        {
            printf("proposer::prepare: failed to create handle for %s\n", nodes[i].c_str());
        }
    }

    return true;
}


void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
    for (unsigned i = 0; i < nodes.size(); i++)
    {
        handle h(nodes[i]);

        if (h.get_rpcc()) {
            int res;
            paxos_protocol::acceptarg arg;

            arg.instance = instance;
            arg.n = my_n;
            arg.v = v;

            printf("proposer::accept: sending acceptreq RPC to %s\n", nodes[i].c_str());

            if (h.get_rpcc()->call(paxos_protocol::acceptreq, me, arg, res, rpcc::to(1000)) == paxos_protocol::OK)
            {
                // FIXME: add support for oldinstance from accept RPC
                if (res)
                {
                    printf("proposer::accept: got acceptres from %s\n", nodes[i].c_str());
                    accepts.push_back(nodes[i]);
                }
                else
                    printf("proposer::accept: got reject from %s\n", nodes[i].c_str());
            }
            else
            {
                printf("proposer::accept: failed to get response from %s\n", nodes[i].c_str());
            }
        }
        else
        {
            printf("proposer::accept: failed to create handle for %s\n", nodes[i].c_str());
        }
    }
}

void
proposer::decide(unsigned instance, std::vector<std::string> nodes,
	      std::string v)
{
    for (unsigned i = 0; i < nodes.size(); i++)
    {
        handle h(nodes[i]);
        int res;
        paxos_protocol::decidearg arg;

        arg.instance = instance;
        arg.v = v;

        printf("proposer::decide: sending decidereq RPC to %s with arg.v=%s\n", nodes[i].c_str(), arg.v.c_str());

        if (h.get_rpcc())
            h.get_rpcc()->call(paxos_protocol::decidereq, me, arg, res, rpcc::to(1000));
        else
            printf("proposer::decide: failed to create handle for %s\n", nodes[i].c_str());
    }
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
    // handle a preparereq message from proposer

    if (a.instance <= instance_h)
    {
        printf("acceptor::preparereq: responding with oldinstance to %s, a.instance = %u, instance_h = %u\n", src.c_str(), a.instance, instance_h);
        r.oldinstance = 1;
        r.accept = instance_h;
        r.v_a = values[instance_h];
    }
    else if (a.n > n_h)
    {
        printf("acceptor::preparereq: responding with prepareres to %s, a.n.n = %u, a.n.m = %s, n_h.n = %u, n_h.m = %s\n", src.c_str(), a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str());

        n_h = a.n;

        r.oldinstance = 0;
        r.accept = 1;
        r.n_a = n_a;
        r.v_a = v_a;

        l->loghigh(n_h);
    }
    else
    {
        printf(" acceptor::preparereq: responding with reject to %s, a.n.n = %u, a.n.m = %s, n_h.n = %u, n_h.m = %s\n", src.c_str(), a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str());

        r.oldinstance = 0;
        r.accept = 0;
    }

    return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r)
{
    // handle an acceptreq message from proposer

    if (a.instance <= instance_h)
    {
        printf("acceptor::acceptreq: responding with oldinstance to %s, a.instance = %u, instance_h = %u\n", src.c_str(), a.instance, instance_h);
        ; // FIXME: add support for oldinstance from accept RPC
    }
    else if (a.n >= n_h)
    {
        printf("acceptor::acceptreq: responding with acceptres to %s, a.n.n = %u, a.n.m = %s, n_h.n = %u, n_h.m = %s\n", src.c_str(), a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str());
        n_a = a.n;
        v_a = a.v;

        l->logprop(a.n, a.v);

        r = 1;
    }
    else
    {
        printf("acceptor::acceptreq: responding with reject to %s, a.n.n = %u, a.n.m = %s, n_h.n = %u, n_h.m = %s\n", src.c_str(), a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str());
        r = 0;
    }

    return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
    // handle an decide message from proposer

    if (a.instance <= instance_h)
    {
        printf("acceptor::decidereq: ignored oldinstance to %s, a.instance = %u, instance_h = %u\n", src.c_str(), a.instance, instance_h);
        ; // ignore the old instance, since it won't matter
    }
    else
    {
        printf("acceptor::decidereq: committing value (decide from %s), a.instance = %u, a.v = %s\n", src.c_str(), a.instance, a.v.c_str());

        commit(a.instance,a.v);
    }

    return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  printf("acceptor::commit: instance=%d, instance_h=%u, value=%s\n", instance, instance_h, value.c_str());
  if (instance > instance_h) {
    printf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  pthread_mutex_lock(&pxs_mutex);
  commit_wo(instance, value);
  pthread_mutex_unlock(&pxs_mutex);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    printf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    printf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    printf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    printf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
