// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server()
{
    int r;
    if (create(0x00000001, r) != extent_protocol::OK)
    {
        // crash on failure
        printf("ERROR: Can't create root directory on the extent server. Peacefully crashing...\n");
        exit(0);
    }
}

int extent_server::create(extent_protocol::extentid_t id, int &)
{
    printf("extent_server::create(id=%lld)\n", id);
    
    if (m_dataBlocks.find(id) != m_dataBlocks.end())
        // TODO: what should we do if the extent exists already?
        return extent_protocol::IOERR;

    // create structure for the new extent
    extent_t e;
    e.data = std::string();
    e.attrs.mtime = e.attrs.atime = e.attrs.ctime = time(NULL);
    e.attrs.size = 0;

    // save structure to the extent map
    m_dataBlocks[id] = e;
    
    return extent_protocol::OK;
}

int extent_server::update(extent_protocol::extentid_t id, std::string buf, unsigned offset, unsigned size, int &)
{
    printf("extent_server::update(id=%lld, buf=%s, offset=%d, size=%d)\n", id, buf.c_str(), offset, size);

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // check if offset is correctly specified
    if (offset > m_dataBlocks[id].attrs.size)
        // TODO: should we change size instead?
        return extent_protocol::IOERR;

    // check if size is correctly specified
    if (offset + size > m_dataBlocks[id].attrs.size)
        size = m_dataBlocks[id].attrs.size - offset;

    // update data in the extent
    m_dataBlocks[id].data.replace(offset, size, buf);

    return extent_protocol::OK;
}

int extent_server::updateAll(extent_protocol::extentid_t id, std::string buf, int &)
{
    printf("extent_server::updateAll(id=%lld, buf=%s)\n", id, buf.c_str());

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // update data in the extent
    m_dataBlocks[id].data = buf;
    m_dataBlocks[id].attrs.size = buf.size();

    return extent_protocol::OK;
}

int extent_server::retrieve(extent_protocol::extentid_t id, unsigned offset, unsigned size, std::string &buf)
{
    printf("extent_server::retrieve(id=%lld, offset=%d, size=%d)\n", id, offset, size);

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // check if offset is correctly specified
    if (offset > m_dataBlocks[id].attrs.size)
        // TODO: should we change size instead?
        return extent_protocol::IOERR;

    // check if size is correctly specified
    if (offset + size > m_dataBlocks[id].attrs.size)
        // TODO: should we still return string of specified size
        // filled with '\0' at positions beyond the file?
        size = m_dataBlocks[id].attrs.size - offset;

    // get data from the extent map
    buf = m_dataBlocks[id].data.substr(offset, size);

    // update access time (simulate relatime behaviour since this is default for
    // Linux since kernel version 2.6.30)
    time_t now = time(NULL);
    if (m_dataBlocks[id].attrs.atime < m_dataBlocks[id].attrs.ctime ||
        m_dataBlocks[id].attrs.atime < m_dataBlocks[id].attrs.mtime ||
        m_dataBlocks[id].attrs.atime < now - 24*60*60)
        m_dataBlocks[id].attrs.atime = now;

    return extent_protocol::OK;
}

int extent_server::retrieveAll(extent_protocol::extentid_t id, std::string &buf)
{
    printf("extent_server::retrieveAll(id=%lld)\n", id);

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    return retrieve(id, 0, m_dataBlocks[id].attrs.size, buf);
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    printf("extent_server::getattr(id=%lld)\n", id);

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // get attributes for the extent
    a = m_dataBlocks[id].attrs;

    return extent_protocol::OK;
}

int extent_server::setattr(extent_protocol::extentid_t id, extent_protocol::attr a, int &)
{
    printf("extent_server::setattr(id=%lld)\n", id);

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // reallocate string buffer if needed
    if (a.size < m_dataBlocks[id].attrs.size)
    {
        std::string newData(m_dataBlocks[id].data, 0, a.size);
        m_dataBlocks[id].data = newData;
    }
    else if (a.size > m_dataBlocks[id].attrs.size)
    {
        std::string newData(a.size, 0);
        newData.replace(0, a.size, m_dataBlocks[id].data);
        m_dataBlocks[id].data = newData;
    }

    // get attributes for the extent
    m_dataBlocks[id].attrs = a;

    return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
    printf("extent_server::remove(id=%lld)\n", id);

    // check if extent exists
    if (m_dataBlocks.find(id) == m_dataBlocks.end())
        return extent_protocol::NOENT;

    // remove it from the extent map
    m_dataBlocks.erase(id);

    return extent_protocol::OK;
}

