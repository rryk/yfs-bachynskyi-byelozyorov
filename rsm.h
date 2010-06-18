// replicated state machine interface.

#ifndef rsm_h
#define rsm_h

#include <string>
#include <vector>
#include "rsm_protocol.h"
#include "rsm_state_transfer.h"
#include "rpc.h"
#include <arpa/inet.h>
#include "config.h"


class rsm : public config_view_change {
 protected:
  std::map<int, handler *> procs;
  config *cfg;
  class rsm_state_transfer *stf;
  rpcs *rsmrpc;
  viewstamp myvs;
  viewstamp last_myvs;
  std::string primary;
  bool insync; 
  bool inviewchange;
  unsigned nbackup;

  // For testing purposes
  rpcs *testsvr;
  bool partitioned;
  bool dopartition;
  bool break1;
  bool break2;


  rsm_client_protocol::status client_members(int i, 
					     std::vector<std::string> &r);
  rsm_protocol::status invoke(int proc, viewstamp vs, std::string mreq, 
			      int &dummy);
  rsm_protocol::status transferreq(std::string src, viewstamp last,
				   rsm_protocol::transferres &r);
  rsm_protocol::status transferdonereq(std::string m, int &r);
  rsm_protocol::status joinreq(std::string src, viewstamp last, 
			       rsm_protocol::joinres &r);
  rsm_test_protocol::status test_net_repairreq(int heal, int &r);
  rsm_test_protocol::status breakpointreq(int b, int &r);

  pthread_mutex_t rsm_mutex;
  pthread_mutex_t invoke_mutex;
  pthread_cond_t recovery_cond;
  pthread_cond_t sync_cond;
  pthread_cond_t join_cond;

  rsm_client_protocol::status client_invoke(int procno, std::string req, 
              std::string &r);
  bool statetransfer(std::string m);
  bool statetransferdone(std::string m);
  bool join(std::string m);
  void set_primary();
  std::string find_highest(viewstamp &vs, std::string &m, unsigned &vid);
  bool sync_with_backups();
  bool sync_with_primary();
  bool amiprimary_wo();
  void net_repair_wo(bool heal);
  void breakpoint1();
  void breakpoint2();
 public:
  rsm (std::string _first, std::string _me);
  ~rsm() {};

  bool amiprimary();
  void set_state_transfer(rsm_state_transfer *_stf) { stf = _stf; };
  void recovery();
  void commit_change();

};

#endif /* rsm_h */
