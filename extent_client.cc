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
    printf("extent_client::create(id=%lld)\n", id);
    pthread_mutex_lock(&localExtents[id].mutex);

    if (localExtents[id].existLocally)
        {
            pthread_mutex_unlock(&localExtents[id].mutex);

            // TODO: what should we do if the extent exists already?
            return extent_protocol::IOERR;
        }

        localExtents[id].buffer = std::string();
        localExtents[id].attrs.mtime = localExtents[id].attrs.atime = localExtents[id].attrs.ctime = time(NULL);
        localExtents[id].attrs.size = 0;
        localExtents[id].isDirty=true;
        localExtents[id].existLocally=true;
        localExtents[id].isRemote=false;

    pthread_mutex_unlock(&localExtents[id].mutex);

    return extent_protocol::OK;
}

extent_protocol::status extent_client::update(extent_protocol::extentid_t id, std::string buf, int offset, int size, int & bytesWritten)
{
    printf("extent_client::update(id=%lld, buf=%s, offset=%d, size=%d)\n", id, buf.c_str(), offset, size);

    pthread_mutex_lock(&localExtents[id].mutex);
        // check if extent exists
        if (! localExtents[id].existLocally)
        {
            extent_protocol::status ret=fetch(id);
            if(ret !=extent_protocol::OK)
            {
                pthread_mutex_unlock(&localExtents[id].mutex);
                return ret;
            }
        }

        // resize string
        if (offset + size > localExtents[id].buffer.size())
        {
            reallocateString(localExtents[id].buffer, offset + size);
            localExtents[id].attrs.size = offset + size;
        }

        // update data in the extent
        localExtents[id].buffer.replace(offset, size, buf);

        // setting modification time
        localExtents[id].attrs.mtime = time(NULL);

        // return number of actual bytes written
        bytesWritten = size;

        localExtents[id].isDirty=true;
    pthread_mutex_unlock(&localExtents[id].mutex);

    return extent_protocol::OK;
}

extent_protocol::status extent_client::updateAll(extent_protocol::extentid_t id, std::string buf)
{
    printf("extent_client::updateAll(id=%lld, buf=%s)\n", id, buf.c_str());

    pthread_mutex_lock(&localExtents[id].mutex);
        // check if extent exists
        if (! localExtents[id].existLocally)
        {
            extent_protocol::status ret=fetch(id);
            if(ret !=extent_protocol::OK)
            {
                pthread_mutex_unlock(&localExtents[id].mutex);
                return ret;
            }
        }

        // update data in the extent
        localExtents[id].buffer = buf;
        localExtents[id].attrs.size = buf.size();

        // setting modification times
        localExtents[id].attrs.mtime = time(NULL);
        localExtents[id].attrs.ctime = time(NULL);

        localExtents[id].isDirty=true;
    pthread_mutex_unlock(&localExtents[id].mutex);

    return extent_protocol::OK;
}

extent_protocol::status extent_client::retrieve(extent_protocol::extentid_t id, int offset, int size, std::string &buf)
{
    printf("extent_client::retrieve(id=%lld, offset=%d, size=%d)\n", id, offset, size);

    pthread_mutex_lock(&localExtents[id].mutex);
        // check if extent exists
        if (! localExtents[id].existLocally)
        {
            extent_protocol::status ret=fetch(id);
            if(ret !=extent_protocol::OK)
            {
                pthread_mutex_unlock(&localExtents[id].mutex);
                return ret;
            }
        }

        // check if offset is correctly specified
        if (offset > localExtents[id].attrs.size)
        {
            pthread_mutex_unlock(&localExtents[id].mutex);

            // TODO: should we change size instead?
            return extent_protocol::IOERR;
        }

        // check if size is correctly specified
        if (offset + size > localExtents[id].attrs.size)
            // TODO: should we still return string of specified size
            // filled with '\0' at positions beyond the file?
            size = localExtents[id].attrs.size - offset;

        // get data from the extent map
        buf = localExtents[id].buffer.substr(offset, size);

        // update access time (simulate relatime behaviour since this is default for
        // Linux since kernel version 2.6.30)
        time_t now = time(NULL);
        if (localExtents[id].attrs.atime < localExtents[id].attrs.ctime ||
            localExtents[id].attrs.atime < localExtents[id].attrs.mtime ||
            localExtents[id].attrs.atime < now - 24*60*60)
            localExtents[id].attrs.atime = now;

    pthread_mutex_unlock(&localExtents[id].mutex);
    return extent_protocol::OK;
}

extent_protocol::status extent_client::retrieveAll(extent_protocol::extentid_t id, std::string &buf)
{
    printf("extent_client::retrieveAll(id=%lld)\n", id);

    pthread_mutex_lock(&localExtents[id].mutex);
        // check if extent exists
        if (! localExtents[id].existLocally)
        {
            extent_protocol::status ret=fetch(id);
            if(ret !=extent_protocol::OK)
            {
                pthread_mutex_unlock(&localExtents[id].mutex);
                return ret;
            }
        }

        buf=localExtents[id].buffer;
    pthread_mutex_unlock(&localExtents[id].mutex);

    return extent_protocol::OK;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    printf("extent_client::getattr(id=%lld)\n", id);

    pthread_mutex_lock(&localExtents[id].mutex);
        // check if extent exists
        if (! localExtents[id].existLocally)
        {
            extent_protocol::status ret=fetch(id);
            if(ret !=extent_protocol::OK)
            {
                pthread_mutex_unlock(&localExtents[id].mutex);
                return ret;
            }
        }

        // get attributes for the extent
        a = localExtents[id].attrs;

        printf(" --> a.size=%d\n", a.size);

    pthread_mutex_unlock(&localExtents[id].mutex);
    return extent_protocol::OK;
}

extent_protocol::status extent_client::setattr(extent_protocol::extentid_t id, extent_protocol::attr a)
{
    printf("extent_client::setattr(id=%lld,a.size=%d)\n", id, a.size);

    pthread_mutex_lock(&localExtents[id].mutex);
        // check if extent exists
        if (! localExtents[id].existLocally)
        {
            extent_protocol::status ret=fetch(id);
            if(ret !=extent_protocol::OK)
            {
                pthread_mutex_unlock(&localExtents[id].mutex);
                return ret;
            }
        }

        // reallocate data buffer if size have changed
        if (a.size != localExtents[id].attrs.size)
            reallocateString(localExtents[id].buffer, a.size);

        // get attributes for the extent
        localExtents[id].attrs = a;

        // setting modification time
        localExtents[id].attrs.mtime = time(NULL);

        localExtents[id].isDirty=true;

    pthread_mutex_unlock(&localExtents[id].mutex);
    return extent_protocol::OK;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t id)
{
    printf("extent_client::remove(id=%lld)\n", id);

    pthread_mutex_lock(&localExtents[id].mutex);
        localExtents[id].existLocally=true;
        localExtents[id].isRemoved=true;
        localExtents[id].isDirty=true;
    pthread_mutex_unlock(&localExtents[id].mutex);
    return extent_protocol::OK;
}

extent_protocol::status extent_client::fetch(extent_protocol::extentid_t id)
{
    extent_protocol::status ret;
    ret = cl->call(extent_protocol::retrieveAll, id, localExtents[id].buffer);
    if (ret!=extent_protocol::OK)
        return ret;
    ret = cl->call(extent_protocol::getattr, id, localExtents[id].attrs);
    if (ret==extent_protocol::OK)
    {
        localExtents[id].isRemote=true;
        localExtents[id].existLocally=true;
    }
    return extent_protocol::OK;
}

void extent_client::reallocateString(std::string &str, unsigned newSize)
{
    printf("extent_client::reallocateString, oldSize=%ld, newSize=%u", str.size(), newSize);
    if (str.size() > newSize)
        str = std::string(str, 0, newSize);
    else if (str.size() < newSize)
    {
        std::string newStr = std::string(newSize, '\0');
        newStr.replace(0, str.size(), str);
        str = newStr;
    }
    printf(", updatedSize=%ld\n", str.size());
}

extent_protocol::status extent_client::flush(extent_protocol::extentid_t id)
{
    int r;
    extent_protocol::attr att;
    pthread_mutex_lock(&localExtents[id].mutex);
        if (localExtents[id].isDirty)
        {
            if (localExtents[id].isRemoved && localExtents[id].isRemote)
                cl->call(extent_protocol::remove,id,r);
            else
                cl->call(extent_protocol::put,id,localExtents[id].buffer, localExtents[id].attrs,r);
        }
        localExtents[id].buffer="";
        localExtents[id].attrs=att;
        localExtents[id].isDirty=false;
        localExtents[id].isRemote=false;
        localExtents[id].isRemoved=false;
        localExtents[id].existLocally=false;
    pthread_mutex_unlock(&localExtents[id].mutex);
    return extent_protocol::OK;
}
