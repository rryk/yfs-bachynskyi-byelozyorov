// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"



class extent_client {
 private:
  rpcc *cl;

  struct extent_t {
      std::string buffer;
      extent_protocol::attr attrs;
      bool isRemote;
      bool isDirty;
      bool existLocally;
      bool isRemoved;
      pthread_mutex_t mutex;

      extent_t():
              isRemote(false),
              isDirty(false),
              existLocally(false),
              isRemoved(false)
      {
          pthread_mutex_init(&mutex, NULL);
      }
  };

  std::map<extent_protocol::extentid_t, extent_t> localExtents; // data blocks
  extent_protocol::status fetch(extent_protocol::extentid_t id);
  void reallocateString(std::string &str, unsigned newSize);


 public:
  extent_client(std::string dst);

  extent_protocol::status create(extent_protocol::extentid_t id);
  extent_protocol::status update(extent_protocol::extentid_t id, std::string buf, int offset, int size, int & bytesWritten);
  extent_protocol::status updateAll(extent_protocol::extentid_t id, std::string buf);
  extent_protocol::status retrieve(extent_protocol::extentid_t id, int offset, int size, std::string &buf);
  extent_protocol::status retrieveAll(extent_protocol::extentid_t id, std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t id, extent_protocol::attr &a);
  extent_protocol::status setattr(extent_protocol::extentid_t id, extent_protocol::attr a);
  extent_protocol::status remove(extent_protocol::extentid_t id);

  extent_protocol::status flush(extent_protocol::extentid_t id);
};

#endif 

