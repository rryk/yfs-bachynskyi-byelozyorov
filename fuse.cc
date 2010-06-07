/*
 * receive request from fuse and call methods of yfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <arpa/inet.h>
#include "yfs_client.h"

int myid;
yfs_client *yfs;

int id() {
  return myid;
}

yfs_client::status
getattr(yfs_client::inum inum, struct stat &st)
{
    yfs_client::status ret=yfs_client::OK;

  bzero(&st, sizeof(st));

  // Get lock
  yfs->acquire(inum);

  st.st_ino = inum;
  printf("getattr %016llx %d\n", inum, yfs->isfile(inum));
  if(yfs->isfile(inum)){
     yfs_client::fileinfo info;
     ret = yfs->getfile(inum, info);
     if(ret != yfs_client::OK)
       goto release;
     st.st_mode = S_IFREG | 0666;
     st.st_nlink = 1;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     st.st_size = info.size;
     printf("   getattr -> %llu\n", info.size);
   } else {
     yfs_client::dirinfo info;
     ret = yfs->getdir(inum, info);
     if(ret != yfs_client::OK)
       goto release;
     st.st_mode = S_IFDIR | 0777;
     st.st_nlink = 2;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     printf("   getattr -> %lu %lu %lu\n", info.atime, info.mtime, info.ctime);
   }

release:
   // Release lock
   yfs->release(inum);
   return ret;
}


void
fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi)
{
    struct stat st;
    yfs_client::inum inum = ino; // req->in.h.nodeid;
    yfs_client::status ret;

    ret = getattr(inum, st);
    if(ret != yfs_client::OK){
      fuse_reply_err(req, ENOENT);
      return;
    }
    fuse_reply_attr(req, &st, 0);
}

void
fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
    printf("fuseserver_setattr 0x%x\n", to_set);
    if (FUSE_SET_ATTR_SIZE & to_set) {

        // Get lock
        yfs->acquire(ino);

        // Update data
        yfs_client::status ret=yfs->setsize(ino, attr->st_size);

        // Release lock
        yfs->release(ino);

        if (ret != yfs_client::OK)
        {
            fuse_reply_err(req, EIO);
            return;
        }

        struct stat st;
        getattr(ino, st);
        fuse_reply_attr(req, &st, 0);
    } else {
        fuse_reply_err(req, ENOSYS);
    }

}

void
fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
      off_t off, struct fuse_file_info *fi)
{
    printf("fuseserver_read(ino=%ld,size=%u,off=%u)\n",ino,size,off);

    std::string buf;

    // Get lock
    yfs->acquire(ino);

    // Retrieve data
    yfs_client::status ret=yfs->retrieve(ino, off, size, buf);

    // Release lock
    yfs->release(ino);

    if (ret != yfs_client::OK)
    {
        printf("Failed to read!!!");
        fuse_reply_err(req, EIO);
        return;
    }

    fuse_reply_buf(req, buf.c_str(), buf.size());
}

void
fuseserver_write(fuse_req_t req, fuse_ino_t ino,
  const char *buf, size_t size, off_t off,
  struct fuse_file_info *fi)
{
    printf("fuseserver_write(ino=%ld,buf=%s,size=%u,off=%ld)\n",ino,buf,size,off);

    int bytesWritten;

    // Get lock
    yfs->acquire(ino);

    // Retrieve data
    yfs_client::status ret=yfs->update(ino, std::string(buf, size), off, size, bytesWritten);

    // Release lock
    yfs->release(ino);

    if (ret != yfs_client::OK)
    {
        printf("Failed to write!!!");
        fuse_reply_err(req, EIO);
        return;
    }

    fuse_reply_write(req, bytesWritten);
}

yfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
     mode_t mode, struct fuse_entry_param *e)
{
    printf("fuseserver_createhelper(parent=%ld,name=%s,mode=%d,e=?)\n",parent,name,mode);


    // Creating 0 response
    yfs_client::status ret;
    bzero(&(e->attr), sizeof(e->attr));
    e->ino=0;
    e->generation=0;
    e->entry_timeout=0.0;
    e->attr_timeout=0.0;

    // Get locks for directory
    yfs->acquire(parent);

    yfs_client::inum fileInum=yfs->ilookup(parent, name);

    if (fileInum==0)
    {
        // Generation of 32 bits inum
        fileInum = random() | 0x80000000;
        printf("fuseserver_createhelper(), generated id: %lld\n", fileInum);

        yfs->acquire(fileInum);

        // Storing file to server
        ret=yfs->create(parent, fileInum, name);
        if (ret!=yfs_client::OK)
            goto release;
    }
    else
    {
        yfs->acquire(fileInum);
    }

    // Getting file information
    yfs_client::fileinfo info;
    ret=yfs->getfile(fileInum,info);
    if (ret!=yfs_client::OK)
        goto release;

    e->ino=fileInum;
    e->generation=1;
    e->attr.st_mode = S_IFREG | 0666;
    e->attr.st_nlink = 1;
    e->attr.st_atime = info.atime;
    e->attr.st_mtime = info.mtime;
    e->attr.st_ctime = info.ctime;
    e->attr.st_size = info.size;

release:
    // Release locks
    yfs->release(fileInum);
    yfs->release(parent);
    return ret;
}

void
fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
   mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_entry_param e;
  if( fuseserver_createhelper( parent, name, mode, &e ) == yfs_client::OK ) {
    fuse_reply_create(req, &e, fi);
  } else {
    fuse_reply_err(req, ENOENT);
  }
}

void fuseserver_mknod( fuse_req_t req, fuse_ino_t parent,
    const char *name, mode_t mode, dev_t rdev ) {
  struct fuse_entry_param e;
  if( fuseserver_createhelper( parent, name, mode, &e ) == yfs_client::OK ) {
    fuse_reply_entry(req, &e);
  } else {
    fuse_reply_err(req, ENOENT);
  }
}

void
fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    printf("fuseserver_lookup(req=?,parent=%ld,name=%s)\n",parent,name);

    struct fuse_entry_param e;
    bzero(&(e.attr), sizeof(e.attr));
    e.ino=0;
    e.generation=0;
    e.entry_timeout=0.0;
    e.attr_timeout=0.0;


    yfs_client::inum parentInum = parent;

    // check whether parent is a dir
    if (!yfs->isdir(parentInum))
    {
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    // Get lock for directory
    yfs->acquire(parentInum);

    // Get data of directory
    yfs_client::inum res = yfs->ilookup(parentInum, name);

    // Release lock of directory
    yfs->release(parentInum);
    
    if (res != 0)
    {
        e.ino = res;
        e.generation = 1;
        
        getattr(res, e.attr);
        fuse_reply_entry(req, &e);
        
        return;
    }
    
    fuse_reply_err(req, ENOENT);
}


struct dirbuf {
    char *p;
    size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_dirent_size(strlen(name));
    b->p = (char *) realloc(b->p, b->size);
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
          off_t off, size_t maxsize)
{
  if (off < bufsize)
    return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
  else
    return fuse_reply_buf(req, NULL, 0);
}

void
fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
          off_t off, struct fuse_file_info *fi)
{
    printf("fuseserver_readdir(req=?,ino=%ld,size=%u,off=%ld,fi=?)\n",ino,size,off);

    yfs_client::inum inum = ino; // req->in.h.nodeid;
    struct dirbuf b;
    yfs_client::dirent e;

    if(!yfs->isdir(inum)){
    fuse_reply_err(req, ENOTDIR);
    return;
    }

    memset(&b, 0, sizeof(b));

    // get listing for the dir
    std::vector<yfs_client::dirent> dirEntries;

    // Get lock for directory
    yfs->acquire(inum);

    // Get directory entries
    yfs->listing(inum, dirEntries);

    // Release lock
    yfs->release(inum);

    // walk over files and add information about them to the FUSE buffer
    for (std::vector<yfs_client::dirent>::const_iterator it =
         dirEntries.begin(); it != dirEntries.end(); it++)
    {
        dirbuf_add(&b, it->name.c_str(), static_cast<fuse_ino_t>(it->inum));
    }

    reply_buf_limited(req, b.p, b.size, off, size);
    free(b.p);
 }


void
fuseserver_open(fuse_req_t req, fuse_ino_t ino,
     struct fuse_file_info *fi)
{
    if (yfs->isdir(ino))
        fuse_reply_err(req, EISDIR);
    fuse_reply_open(req, fi);
}

void
fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
     mode_t mode)
{
    struct fuse_entry_param e;

    printf("fuseserver_mkdir(parent=%ld,name=%s,mode=%d,e=?)\n",parent,name,mode);

    // Creating 0 response
    yfs_client::status r;
    bzero(&(e.attr), sizeof(e.attr));
    e.ino=0;
    e.generation=0;
    e.entry_timeout=0.0;
    e.attr_timeout=0.0;

    // Generation of 32 bits inum
    yfs_client::inum dirINum = random() & 0x7fffffff;
    printf("fuseserver_mkdir(), generated id: %lld\n", dirINum);

    // Get locks for directories
    yfs->acquire(dirINum);
    yfs->acquire(parent);

    // Storing dir to server
    r=yfs->create(parent, dirINum, name);
    if (r!=yfs_client::OK)
    {
        fuse_reply_err(req,ENOENT);
    }
    else
    {
        // Getting dir information
        yfs_client::dirinfo info;
        r=yfs->getdir(dirINum,info);
        if (r!=yfs_client::OK)
        {
            fuse_reply_err(req,EIO);
        }
        else
        {
            e.ino=dirINum;
            e.generation=1;
            e.attr.st_mode = S_IFDIR | 0777;
            e.attr.st_nlink = 2;
            e.attr.st_atime = info.atime;
            e.attr.st_mtime = info.mtime;
            e.attr.st_ctime = info.ctime;

            fuse_reply_entry(req, &e);
        }
    }

    // Release locks
    yfs->release(dirINum);
    yfs->release(parent);

}

void
fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    // Getting lock for directory
    yfs->acquire(parent);

    // Gettin inum of file
    yfs_client::inum fileInum=yfs->ilookup(parent,name);

    // Getting lock for file
    yfs->acquire(fileInum);

    // Removing file from directory and disk
    yfs_client::status res = yfs->remove(parent, name);

    // Release locks
    yfs->release(parent);
    yfs->release(fileInum);

    // Checking for errors
    if (res == yfs_client::NOENT)
        fuse_reply_err(req, ENOENT);
    else if (res == yfs_client::IOERR)
        fuse_reply_err(req, EIO);
    else
        fuse_reply_err(req, 0);
}

void
fuseserver_statfs(fuse_req_t req)
{
  struct statvfs buf;

  printf("statfs\n");

  memset(&buf, 0, sizeof(buf));

  buf.f_namemax = 255;
  buf.f_bsize = 512;

  fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

int
main(int argc, char *argv[])
{
  char *mountpoint = 0;
  int err = -1;
  int fd;

  setvbuf(stdout, NULL, _IONBF, 0);

  if(argc != 4){
    fprintf(stderr, "Usage: yfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
    exit(1);
  }
  mountpoint = argv[1];

  srandom(getpid());

  myid = random();

  yfs = new yfs_client(argv[2], argv[3]);

  fuseserver_oper.getattr    = fuseserver_getattr;
  fuseserver_oper.statfs     = fuseserver_statfs;
  fuseserver_oper.readdir    = fuseserver_readdir;
  fuseserver_oper.lookup     = fuseserver_lookup;
  fuseserver_oper.create     = fuseserver_create;
  fuseserver_oper.mknod      = fuseserver_mknod;
  fuseserver_oper.open       = fuseserver_open;
  fuseserver_oper.read       = fuseserver_read;
  fuseserver_oper.write      = fuseserver_write;
  fuseserver_oper.setattr    = fuseserver_setattr;
  fuseserver_oper.unlink     = fuseserver_unlink;
  fuseserver_oper.mkdir      = fuseserver_mkdir;

  const char *fuse_argv[20];
  int fuse_argc = 0;
  fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

  // everyone can play, why not?
  //fuse_argv[fuse_argc++] = "-o";
  //fuse_argv[fuse_argc++] = "allow_other";

  fuse_argv[fuse_argc++] = mountpoint;
  fuse_argv[fuse_argc++] = "-d";

  fuse_args args = FUSE_ARGS_INIT( fuse_argc, (char **) fuse_argv );
  int foreground;
  int res = fuse_parse_cmdline( &args, &mountpoint, 0 /*multithreaded*/,
        &foreground );
  if( res == -1 ) {
    fprintf(stderr, "fuse_parse_cmdline failed\n");
    return 0;
  }

  args.allocated = 0;

  fd = fuse_mount(mountpoint, &args);
  if(fd == -1){
    fprintf(stderr, "fuse_mount failed\n");
    exit(1);
  }

  struct fuse_session *se;

  se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
       NULL);
  if(se == 0){
    fprintf(stderr, "fuse_lowlevel_new failed\n");
    exit(1);
  }

  struct fuse_chan *ch = fuse_kern_chan_new(fd);
  if (ch == NULL) {
    fprintf(stderr, "fuse_kern_chan_new failed\n");
    exit(1);
  }

  fuse_session_add_chan(se, ch);
  // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
  err = fuse_session_loop(se);

  fuse_session_destroy(se);
  close(fd);
  fuse_unmount(mountpoint);

  return err ? 1 : 0;
}
