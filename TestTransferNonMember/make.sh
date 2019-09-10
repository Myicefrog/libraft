g++ -std=c++11 -o TestTransferNonMember TestTransferNonMember.cc raft_test_util.cc -I ../include/ -I ../src/  -L ../lib/ -L /usr/local/lib -lraft -lprotobuf -lpthread
