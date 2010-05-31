// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
        make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status extent_client::create(extent_protocol::extentid_t id)
{
    extent_protocol::status ret;
    int r;
    ret = cl->call(extent_protocol::create, id, r);
    return ret;
}

extent_protocol::status extent_client::update(extent_protocol::extentid_t id, std::string buf, int offset, int size, int & bytesWritten)
{
    extent_protocol::status ret;
    ret = cl->call(extent_protocol::update, id, buf, offset, size, bytesWritten);
    return ret;
}

extent_protocol::status extent_client::updateAll(extent_protocol::extentid_t id, std::string buf)
{
    extent_protocol::status ret;
    int r;
    ret = cl->call(extent_protocol::updateAll, id, buf, r);
    return ret;
}

extent_protocol::status extent_client::retrieve(extent_protocol::extentid_t id, int offset, int size, std::string &buf)
{
    extent_protocol::status ret;
    ret = cl->call(extent_protocol::retrieve, id, offset, size, buf);
    return ret;
}

extent_protocol::status extent_client::retrieveAll(extent_protocol::extentid_t id, std::string &buf)
{
    extent_protocol::status ret;
    ret = cl->call(extent_protocol::retrieveAll, id, buf);
    return ret;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    extent_protocol::status ret;
    ret = cl->call(extent_protocol::getattr, id, a);
    return ret;
}

extent_protocol::status extent_client::setattr(extent_protocol::extentid_t id, extent_protocol::attr a)
{
    extent_protocol::status ret;
    int r;
    ret = cl->call(extent_protocol::setattr, id, a, r);
    return ret;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t id)
{
    extent_protocol::status ret;
    int r;
    ret = cl->call(extent_protocol::remove, id, r);
    return ret;
}
