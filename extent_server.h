// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

struct extent_t {
    std::string data;
    extent_protocol::attr attrs;
};

class extent_server {

    std::map<extent_protocol::extentid_t, extent_t> m_dataBlocks; // data blocks

 public:
  extent_server();

  // create an extent
  int create(extent_protocol::extentid_t id, int &);

  // update extent content
  int update(extent_protocol::extentid_t id, std::string buf, unsigned offset, unsigned size, int & bytesWritten);

  // update full extent content (with resize)
  int updateAll(extent_protocol::extentid_t id, std::string buf, int &);

  // get extent content
  int retrieve(extent_protocol::extentid_t id, unsigned offset, unsigned size, std::string &buf);

  // get full extent content
  int retrieveAll(extent_protocol::extentid_t id, std::string &buf);

  // get extent attributes
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);

  // set extent attributes
  int setattr(extent_protocol::extentid_t id, extent_protocol::attr, int &);

  // remove an extent
  int remove(extent_protocol::extentid_t id, int &);

  // put extent with attrs to server
  int put(extent_protocol::extentid_t id, std::string buf, extent_protocol::attr a, int &);

private:
  void reallocateString(std::string &str, unsigned newSize);
};

#endif 







