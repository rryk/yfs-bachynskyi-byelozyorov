#include "rpc.h"
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include "extent_server.h"

// Main loop of extent server

int
main(int argc, char *argv[])
{
  int count = 0;

  if(argc != 2){
    fprintf(stderr, "Usage: %s port\n", argv[0]);
    exit(1);
  }

  setvbuf(stdout, NULL, _IONBF, 0);

  char *count_env = getenv("RPC_COUNT");
  if(count_env != NULL){
    count = atoi(count_env);
  }

  rpcs server(atoi(argv[1]), count);
  extent_server ls;

  server.reg(extent_protocol::create, &ls, &extent_server::create);
  server.reg(extent_protocol::update, &ls, &extent_server::update);
  server.reg(extent_protocol::updateAll, &ls, &extent_server::updateAll);
  server.reg(extent_protocol::retrieve, &ls, &extent_server::retrieve);
  server.reg(extent_protocol::retrieveAll, &ls, &extent_server::retrieveAll);
  server.reg(extent_protocol::getattr, &ls, &extent_server::getattr);
  server.reg(extent_protocol::setattr, &ls, &extent_server::setattr);
  server.reg(extent_protocol::remove, &ls, &extent_server::remove);
  server.reg(extent_protocol::put, &ls, &extent_server::put);

  while(1)
    sleep(1000);
}
