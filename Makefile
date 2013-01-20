CC := g++
CFLAGS := -Wall -pedantic -g
INCLUDE_PATH := -I/home/Andrew/dev/boost_1_51_0 -I/usr/include/python2.7

RM := rm -rf

LIBS := -lboost_system -lboost_thread -lboost_program_options -lboost_filesystem -lpython2.7 -lpthread -lrt
LIB_PATH := /home/Andrew/dev/boost_1_51_0/stage/lib

objdir := objs
OUT := PythonServer PyExec PythonSender
OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(OUT)))

all: installdirs $(OUT)

installdirs:
	mkdir -p $(objdir)

$(OUT): $(OBJS)
	$(eval main_obj= $(addprefix $(objdir)/, $(addsuffix .o, $@)))
	$(CC) $(INCLUDE_PATH) -L$(LIB_PATH) $(LIBS) $(CFLAGS)  $(main_obj) -o $@

$(objdir)/%.o: %.cpp
	@echo Compiling $<
	$(CC) $(INCLUDE_PATH) $(CFLAGS) -c $< -o $@

clean:
	$(RM) $(OUT) $(objdir)


.PHONY: all clean installdirs
