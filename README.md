# libraft_improve_

C++ raft lib

# 说明

从https://github.com/lichuang/libraft上fork下来的。原作者把etcd的raft移植为了c++

但有一些编译问题，所以我修改了makefile文件。

# 环境要求

gcc（4.8.5以上）

gtest

- gtest编译时候，生成动态库使用。使用静态库容易出现pthread链接问题。 cmake -DBUILD_SHARED_LIBS=ON .

protobuf

# 编译

不使用make，使用make libso，生成so文件


# 关于Test目录说明

原作者把例子写成了TestCase，我为了调试方便，为单独的case都写了一个main

TestDuelingCandidates开始研究
