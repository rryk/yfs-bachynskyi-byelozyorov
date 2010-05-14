// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;


  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

yfs_client::inum yfs_client::ilookup(inum di, std::string name)
{
    // read listing for the dir
    std::vector<yfs_client::dirent> dirEntries;
    int res = listing(di, dirEntries);
    
    // check whether listing was retrieved successfully
    if (res != OK)
        return 0;
    
    // search for the file
    for (std::vector<dirent>::const_iterator it = dirEntries.begin(); it != dirEntries.end(); it++)
        if (it->name.compare(name) == 0)
            return it->inum;
            
    return 0;
}

int
yfs_client::listing(inum inum, std::vector<dirent> & entries)
{
    printf("yfs_client::listing %016llx\n", inum);

    // Get directory content
    std::string buf;
    if (ec->retrieveAll(inum, buf) != extent_protocol::OK)
        // failed to read dir
        return IOERR;

    /* Dir structure format:
     *   filename1:inum1:filename2:inum2:filename3:inum3...
     */

    // Create mutable copy of the string as needed by strtok
    char dirContent[buf.length()];
    strcpy(dirContent, buf.c_str());

    // Read until there are more files
    const char * filenameStr = strtok(dirContent, ":");
    const char * inumStr;
    while (filenameStr != NULL)
    {
        // get inum for the file
        inumStr = strtok(NULL, ":");

        // check that inum for the file exists
        assert(inumStr != NULL);

        dirent e;
        e.name = filenameStr;
        e.inum = n2i(inumStr);

        // add new entry
        entries.push_back(e);

        // get next file
        filenameStr = strtok(NULL, ":");

    }

    return OK;
}

int yfs_client::create(inum parentINum, inum fileINum, const char * fileName)
{
    printf("yfs_client::create %016llx in directory %016llx\n", fileINum, parentINum);

    // Add file with inum to server
    if (ec->create(fileINum) != extent_protocol::OK)
        return IOERR;

    // Get directory content from server
    std::string dirContent;
    if (ec->retrieveAll(parentINum, dirContent) != extent_protocol::OK)
        // failed to read dir content
        return NOENT;

    // Append information about file to directory information
    if (!dirContent.empty())
        dirContent.append(":");
    dirContent.append(fileName).append(":").append(filename(fileINum));

    // Update directory content on the server
    if (ec->updateAll(parentINum, dirContent) != extent_protocol::OK)
        // failed to update dir
        return IOERR;

    return OK;
}

int yfs_client::update(inum fileINum, std::string content, int offset, int size, int & bytesWritten)
{
    printf("yfs_client::update %016llx\n", fileINum);

    // Update file content
    if (ec->update(fileINum, content, offset, size, bytesWritten) != extent_protocol::OK)
        // failed to write to the file
        return IOERR;

    return OK;
}

int yfs_client::retrieve(inum fileINum, int offset, int size, std::string &content)
{
    printf("yfs_client::update %016llx\n", fileINum);

    // Retrieve file content
    if (ec->retrieve(fileINum, offset, size, content) != extent_protocol::OK)
        // failed to write to the file
        return IOERR;

    return OK;
}

int yfs_client::setsize(inum fileINum, int newSize)
{
    printf("yfs_client::setattr %016llx\n", fileINum);

    // Get current attr
    extent_protocol::attr attr;
    if (ec->getattr(fileINum, attr) != extent_protocol::OK)
        // failed to read current attributes from file/dir
        return IOERR;

    // Set new size
    attr.size = newSize;

    // Update file/dir attributes on the server
    if (ec->setattr(fileINum, attr) != extent_protocol::OK)
        // failed to update attributes for the file/dir
        return IOERR;

    return OK;
}

int yfs_client::remove(inum parentINum, const char * fileName)
{
    printf("yfs_client::remove %s in %016llx", fileName, parentINum);
    
    inum res = ilookup(parentINum, std::string(fileName));
    if (res == 0)
        return NOENT;
        
    if (ec->remove(res) != extent_protocol::OK)
        return IOERR;
        
    return OK;
    
}
