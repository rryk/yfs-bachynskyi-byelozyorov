LAB=8
SOL=0
RPC=./rpc
LAB2GE=$(shell expr $(LAB) \>\= 2)
LAB4GE=$(shell expr $(LAB) \>\= 4)
LAB5GE=$(shell expr $(LAB) \>\= 5)
LAB6GE=$(shell expr $(LAB) \>\= 6)
LAB7GE=$(shell expr $(LAB) \>\= 7)
LAB8GE=$(shell expr $(LAB) \>\= 8)
CXXFLAGS =  -g -MD -Wall -I. -I$(RPC) -DLAB=$(LAB) -DSOL=$(SOL) -D_FILE_OFFSET_BITS=64
FUSEFLAGS= -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=25 -I/usr/local/include/fuse -I/usr/include/fuse
ifeq ($(shell uname -s),Darwin)
MACFLAGS= -D__FreeBSD__=10
else
MACFLAGS=
endif
LDFLAGS = -L. -L/usr/local/lib
LDLIBS = -lpthread 
ifeq ($(LAB2GE),1)
ifeq ($(shell uname -s),Darwin)
ifeq ($(shell sw_vers -productVersion | sed -e "s/.*\(10\.[0-9]\).*/\1/"),10.6)
LDLIBS += -lfuse_ino64
else
LDLIBS += -lfuse
endif
else
LDLIBS += -lfuse
endif
endif
LDLIBS += $(shell test -f `gcc -print-file-name=librt.so` && echo -lrt)
LDLIBS += $(shell test -f `gcc -print-file-name=libdl.so` && echo -ldl)
CC = g++
CXX = g++

lab:  lab8
lab1: rpc/rpctest lock_server lock_tester lock_demo
lab2: yfs_client extent_server
lab3: yfs_client extent_server
lab4: yfs_client extent_server lock_server test-lab-4-b test-lab-4-c
lab5: yfs_client extent_server lock_server lock_tester test-lab-4-b\
	 test-lab-4-c
lab6: yfs_client extent_server lock_server test-lab-4-b test-lab-4-c
lab7: lock_server rsm_tester
lab8: lock_tester lock_server rsm_tester yfs_client extent_server test-lab-4-b test-lab-4-c

hfiles1=rpc/fifo.h rpc/connection.h rpc/rpc.h rpc/marshall.h rpc/method_thread.h\
	rpc/thr_pool.h rpc/pollmgr.h rpc/jsl_log.h rpc/slock.h rpc/rpctest.cc\
	lock_protocol.h lock_server.h lock_client.h gettime.h gettime.cc
hfiles2=yfs_client.h extent_client.h extent_protocol.h extent_server.h
hfiles3=lock_client_cache.h lock_server_cache.h
hfiles4=log.h rsm.h rsm_protocol.h config.h paxos.h paxos_protocol.h rsm_state_transfer.h handle.h rsmtest_client.h
hfiles5=rsm_state_transfer.h rsm_client.h
rsm_files = rsm.cc paxos.cc config.cc log.cc handle.cc

rpclib=rpc/rpc.cc rpc/connection.cc rpc/pollmgr.cc rpc/thr_pool.cc rpc/jsl_log.cc gettime.cc
rpc/librpc.a: $(patsubst %.cc,%.o,$(rpclib))
	rm -f $@
	ar cq $@ $^
	ranlib rpc/librpc.a

rpc/rpctest=rpc/rpctest.cc
rpc/rpctest: $(patsubst %.cc,%.o,$(rpctest)) rpc/librpc.a

lock_demo=lock_demo.cc lock_client.cc
lock_demo : $(patsubst %.cc,%.o,$(lock_demo)) rpc/librpc.a

lock_tester=lock_tester.cc lock_client.cc
ifeq ($(LAB5GE),1)
lock_tester += lock_client_cache.cc
endif
ifeq ($(LAB8GE),1)
lock_tester+=rsm_client.cc
endif
lock_tester : $(patsubst %.cc,%.o,$(lock_tester)) rpc/librpc.a

lock_server=lock_server.cc lock_smain.cc
ifeq ($(LAB5GE),1)
lock_server+=lock_server_cache.cc
endif
ifeq ($(LAB7GE),1)
lock_server+= $(rsm_files)
endif
lock_server : $(patsubst %.cc,%.o,$(lock_server)) rpc/librpc.a

yfs_client=yfs_client.cc extent_client.cc fuse.cc
ifeq ($(LAB4GE),1)
yfs_client += lock_client.cc
endif
ifeq ($(LAB8GE),1)
yfs_client += rsm_client.cc
endif
ifeq ($(LAB5GE),1)
yfs_client += lock_client_cache.cc
endif
yfs_client : $(patsubst %.cc,%.o,$(yfs_client)) rpc/librpc.a

extent_server=extent_server.cc extent_smain.cc
extent_server : $(patsubst %.cc,%.o,$(extent_server)) rpc/librpc.a

test-lab-4-b=test-lab-4-b.c
test-lab-4-b:  $(patsubst %.c,%.o,$(test_lab_4-b)) rpc/librpc.a

test-lab-4-c=test-lab-4-c.c
test-lab-4-c:  $(patsubst %.c,%.o,$(test_lab_4-c)) rpc/librpc.a

rsm_tester=rsm_tester.cc rsmtest_client.cc
rsm_tester:  $(patsubst %.c,%.o,$(rsm_tester)) rpc/librpc.a

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

fuse.o: fuse.cc
	$(CXX) -c $(CXXFLAGS) $(FUSEFLAGS) $(MACFLAGS) $<

l1:
	./mklab.pl 1 0 l1 GNUmakefile $(rpclib) $(rpctest) $(lock_server)\
	 $(lock_demo) $(lock_tester) $(hfiles1)

l1-sol:
	./mklab.pl 1 1 l1-sol GNUmakefile $(rpclib) $(rpctest) $(lock_server)\
	 $(lock_demo) $(lock_tester) $(hfiles1)

l2:
	./mklab.pl 2 0 l2 GNUmakefile $(rpclib) $(yfs_client) $(extent_server) $(lock_server) start.sh\
	 stop.sh test-lab-2.pl mkfs.sh $(hfiles2) $(hfiles1)

l2-sol:
	./mklab.pl 2 2 l2-sol GNUmakefile $(yfs_client) $(extent_server) $(rpclib) $(lock_server) start.sh\
	 stop.sh test-lab-2.pl mkfs.sh $(hfiles2) $(hfiles1)

l3:
	./mklab.pl 3 0 l3 GNUmakefile  $(rpclib) $(yfs_client) $(extent_server) $(lock_server) start.sh\
	 stop.sh test-lab-2.pl mkfs.sh $(hfiles2) $(hfiles1) test-lab-3.pl

l3-sol:
	./mklab.pl 3 3 l3-sol GNUmakefile  $(yfs_client) $(extent_server) $(rpclib) $(lock_server) start.sh\
	 stop.sh test-lab-2.pl mkfs.sh $(hfiles2) $(hfiles1) test-lab-3.pl

l4:
	./mklab.pl 4 0 l4 GNUmakefile test-lab-4-a.pl $(rpclib) $(yfs_client) $(rpctest) $(lock_server)\
	 $(extent_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
	 $(lock_demo) $(lock_tester) $(hfiles1) $(hfiles2) $(test-lab-4-b) $(test-lab-4-c)

l4-sol:
	./mklab.pl 4 4 l4-sol GNUmakefile test-lab-4-a.pl $(yfs_client) $(rpclib) $(rpctest) $(lock_server)\
	 $(extent_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
	 $(lock_demo) $(lock_tester) $(hfiles1) $(hfiles2) $(test-lab-4-b) $(test-lab-4-c)

l5:
	./mklab.pl 5 0 l5 GNUmakefile test-lab-4-a.pl $(rpclib) $(yfs_client)\
	 $(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl \
	 mkfs.sh $(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) \
	 $(test-lab-4-b) $(test-lab-4-c)

l5-sol:
	./mklab.pl 5 5 l5-sol GNUmakefile test-lab-4-a.pl $(yfs_client) $(rpclib) $(rpctest)\
	 $(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl \
	 mkfs.sh $(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) \
	 $(test-lab-4-b) $(test-lab-4-c)


l6:
	./mklab.pl 6 0 l6 GNUmakefile test-lab-4-a.pl $(rpclib) $(yfs_client) $(rpctest)\
	 $(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
	 $(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) $(test-lab-4-b) $(test-lab-4-c)

l6-sol:
	./mklab.pl 6 6 l6-sol GNUmakefile test-lab-4-a.pl $(yfs_client) $(rpclib) $(rpctest)\
	 $(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
	 $(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) $(test-lab-4-b) $(test-lab-4-c)


l7:
	./mklab.pl 7 0 l7 GNUmakefile rsm_tester.pl $(yfs_client) $(rpclib) $(rpctest)\
		$(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
		$(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) $(test-lab-4-b) $(test-lab-4-c)\
		rsm_tester.pl $(rsm_files) $(hfiles4) $(rsm_tester)

l7-sol:
	./mklab.pl 7 7 l7-sol GNUmakefile test-lab-4-a.pl $(yfs_client) $(rpclib) $(rpctest)\
		$(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
		$(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) $(test-lab-4-b) $(test-lab-4-c)\
		rsm_tester.pl $(rsm_files) $(hfiles4) $(rsm_tester)

l8:
	./mklab.pl 8 0 l8 GNUmakefile rsm_client.cc $(yfs_client) $(rpclib) $(rpctest)\
		$(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
		$(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) $(test-lab-4-b) $(test-lab-4-c)\
		rsm_tester.pl $(rsm_files) $(hfiles4) rsm_client.cc $(hfiles5) $(rsm_tester)

l8-sol:
	./mklab.pl 8 8 l8-sol GNUmakefile test-lab-4-a.pl $(yfs_client) $(rpclib) $(rpctest)\
		$(extent_server) $(lock_server) start.sh stop.sh test-lab-2.pl mkfs.sh\
		$(hfiles1) $(hfiles2) $(hfiles3) $(lock_tester) $(test-lab-4-b) $(test-lab-4-c)\
		rsm_tester.pl $(rsm_files) $(hfiles4) rsm_client.cc $(hfiles5) $(rsm_tester)

-include *.d

.PHONY : clean
clean : 
	rm -rf rpc/rpctest rpc/*.o rpc/*.d rpc/librpc.a *.o *.d yfs_client extent_server lock_server lock_tester lock_demo rpctest test-lab-4-b test-lab-4-c rsm_tester
