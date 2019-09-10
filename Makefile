DIR=.
BIN_DIR=$(DIR)/bin
SRC_DIR=$(DIR)/src
TEST_DIR=$(DIR)/test
INCLUDE_DIR=$(DIR)/
OBJ_DIR=$(DIR)/obj
LIB_DIR=$(DIR)/lib
LIB=libraft.a
APP=libraft.so

EXTENSION=cc
OBJS=$(patsubst $(SRC_DIR)/%.$(EXTENSION), $(OBJ_DIR)/%.o,$(wildcard $(SRC_DIR)/*.$(EXTENSION)))
TEST_OBJS=$(patsubst $(TEST_DIR)/%.$(EXTENSION), $(TEST_DIR)/%.o,$(wildcard $(TEST_DIR)/*.$(EXTENSION)))
DEPS=$(patsubst $(OBJ_DIR)/%.o, $(DEPS_DIR)/%.d, $(OBJS))

INCLUDE=-I ./include -I ./src
		
CC=g++
AR= ar rcu
#CFLAGS=-std=c99 -Wall -Werror -g 
CFLAGS=-std=c++11 -Wall -Wformat=0  -g -fno-strict-aliasing -O0 -export-dynamic -Wall -pipe  -D_GNU_SOURCE -D_REENTRANT -fPIC -Wno-deprecated -m64 
LDFLAGS= -L. -L /usr/local/lib

all:lib

lib:$(OBJS)
	$(AR) $(LIB_DIR)/$(LIB) $(OBJS) 

libso:$(OBJS)
	$(CC) -shared -o ${APP}  $(OBJS) 


test:$(TEST_OBJS)
	##$(CC) $(TEST_OBJS) -o test/all_test $(LIB_DIR)/$(LIB) -lprotobuf -lgtest -lpthread -lm
	$(CC) ${LDFLAGS} $(TEST_OBJS) -o test/all_test -lraft -lprotobuf -lgtest -lpthread -lm

$(TEST_DIR)/%.o:$(TEST_DIR)/%.$(EXTENSION)
	$(CC) $< -o $@ -c -fPIC $(CFLAGS) $(INCLUDE)

$(OBJ_DIR)/%.o:$(SRC_DIR)/%.$(EXTENSION) 
	$(CC) $< -o $@ -c -fPIC $(CFLAGS) $(INCLUDE) 

rebuild:
	make clean
	make

clean:
	rm -rf $(TEST_OBJS) $(OBJS) $(BIN_DIR)/* $(LIB_DIR)/* $(TEST_DIR)/all_test
