// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
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
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

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

int
yfs_client::getlisting(inum inum, std::vector<dirent> & entries)
{
    int r = OK;

    const char * filenameStr;
    const char * inumStr;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;

    std::string buf;
    if (ec->get(inum, buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    /* Dir structure format:
     *   filename1:inum1:filename2:inum2:filename3:inum3...
     */

    // create mutable copy of the string as needed by strtok
    char * dirContent;
    strcpy(dirContent, buf.c_str());

    filenameStr = strtok(dirContent, ":");

    // read until there are more files
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

release:
    return r;
}



