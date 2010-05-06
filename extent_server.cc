// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    printf("extent_server::put(id=%lld)\n", id);

    return extent_protocol::IOERR;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
    printf("extent_server::get(id=%lld)\n", id);

    return extent_protocol::IOERR;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    printf("extent_server::getattr(id=%lld)\n", id);

    a.size = 0;
    a.atime = 0;
    a.mtime = 0;
    a.ctime = 0;
    return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
    printf("extent_server::remove(id=%lld)\n", id);

    return extent_protocol::IOERR;
}

