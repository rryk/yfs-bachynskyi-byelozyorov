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

    // check for the root dir
    extent_protocol::attr attr;
    if (ec->getattr(0x00000001, attr) == extent_protocol::NOENT)
    {
        // create root dir
        if (ec->put(0x00000001, "") != extent_protocol::OK)
        {
            // crash on failure
            printf("ERROR: Can't create root directory on the extent server. Peacefully crashing...\n");
            exit(0);
        }
    }
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
    printf("getlisting %016llx\n", inum);

    // get directory content
    std::string buf;
    if (ec->get(inum, buf) != extent_protocol::OK)
        return IOERR;

    /* Dir structure format:
     *   filename1:inum1:filename2:inum2:filename3:inum3...
     */

    // create mutable copy of the string as needed by strtok
    char * dirContent = new char[buf.length()];
    strcpy(dirContent, buf.c_str());

    printf("getlisting %016llx -> dirContent = `%s`\n", inum, dirContent);

    // read until there are more files
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

        printf("yfs_client::getlisting filename %s, inum %016llx \n",filenameStr, e.inum);
        // add new entry
        entries.push_back(e);

        // get next file
        filenameStr = strtok(NULL, ":");
    }

    // free memory
    delete [] dirContent;

    return OK;
}

int yfs_client::putfile(inum parentINum, const char * fileName, inum fileINum, std::string content)
{
    std::string dirContent;
    printf("putfile %016llx in directory %016llx\n", fileINum, parentINum);

    // Add file with inum to server
    if (ec->put(fileINum, content) != extent_protocol::OK)
        return IOERR;

    // Get directory content from server
    if (ec->get(parentINum, dirContent) != extent_protocol::OK)
        return NOENT;

    // Append information about file to directory information
    if (! dirContent.empty())
        dirContent.append(":");
    dirContent.append(fileName).append(":").append(filename(fileINum));

    // Add changed directory content to server
    if (ec->put(parentINum, dirContent) != extent_protocol::OK)
        return IOERR;

    return OK;
}
