g++ -std=c++11 -o TestNodePropose TestNodePropose.cc raft_test_util.cc -I ../include/ -I ../src/  -L ../lib/ -L /usr/local/lib -lraft -lprotobuf -lpthread
