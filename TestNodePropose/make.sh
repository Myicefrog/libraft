g++ -std=c++11 -o TestNodePropose TestNodePropose.cc ../test/raft_test_util.cc -I ../include/ -I ../src/ -I ../test/ -L ../lib/ -L /usr/local/lib -lraft -lprotobuf -lpthread
