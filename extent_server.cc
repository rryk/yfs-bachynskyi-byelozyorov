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

    // if extent exists already -- we simply overwrite it
    // as would happen in typical file system

    // create structure for new extent
    extent_t e;
    e.data = buf;
    e.attrs.mtime = e.attrs.atime = e.attrs.ctime = time(NULL);
    e.attrs.size = buf.length();

    // save structure to the extent map
    m_dataBlocks[id] = e;

    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
    printf("extent_server::get(id=%lld)\n", id);

    // check if element exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // get data from the extent map
    buf = m_dataBlocks[id].data;

    // simulate _relatime_ behaviour since this is default for Linux
    // since kernel version 2.6.30
    time_t now = time(NULL);
    if (m_dataBlocks[id].attrs.atime < m_dataBlocks[id].attrs.ctime ||
        m_dataBlocks[id].attrs.atime < m_dataBlocks[id].attrs.mtime ||
        m_dataBlocks[id].attrs.atime < now - 24*60)
        m_dataBlocks[id].attrs.atime = now;

    return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    printf("extent_server::getattr(id=%lld)\n", id);

    // check if element exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // get attributes from the extent map
    a = m_dataBlocks[id].attrs;

    return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
    printf("extent_server::remove(id=%lld)\n", id);

    // check if element exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // remove it from the extent map
    m_dataBlocks.erase(id);

    return extent_protocol::OK;
}

