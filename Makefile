CC := g++
RFLAGS := -Wall -Wextra -Wno-unused-parameter -pedantic -pthread -O3
DFLAGS := -Wall -Wextra -Wno-unused-parameter -pedantic -pthread -O0 -ggdb

ifneq ($(MAKECMDGOALS),debug)
	CFLAGS := $(RFLAGS)
else
	CFLAGS := $(DFLAGS)
endif

INCLUDE_PATH := -I/usr/include/boost -Isrc

RM := rm -rf

LIBS := -lboost_system -lboost_thread -lboost_program_options -lboost_filesystem -lrt
LIB_PATH := /usr/lib


srcdir := src
objdir := objs
depdir := deps

common_dir := $(srcdir)/common
worker_dir := $(srcdir)/worker
master_dir := $(srcdir)/master

COMMON_WORKER := log daemon config protocol common master_ping job_completion_ping job_completion_table
COMMON_WORKER_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(COMMON_WORKER)))

WORKER := pyexec pyserver
WORKER_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(WORKER)))

COMMON_MASTER := log daemon config protocol defines ping node_ping job worker job_manager worker_manager sheduler job_sender result_getter timeout_manager admin
COMMON_MASTER_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(COMMON_MASTER)))

MASTER := master
MASTER_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(MASTER)))

OUT_WORKER := pyserver pyexec
OUT_MASTER := master

ALL_CPP := $(COMMON_WORKER) $(COMMON_MASTER) $(WORKER) $(MASTER)
DEPENDS := $(addprefix $(depdir)/, $(addsuffix .d, $(ALL_CPP)))


all: installdirs $(DEPENDS) $(OUT_WORKER) $(OUT_MASTER)

debug: all

installdirs:
	@mkdir -p $(objdir) $(depdir)

$(OUT_WORKER): $(COMMON_WORKER_OBJS) $(WORKER_OBJS)
	$(eval main_obj= $(addprefix $(objdir)/, $(addsuffix .o, $@)))
	$(CC) $(main_obj) $(COMMON_WORKER_OBJS) -o $@ $(INCLUDE_PATH) -L$(LIB_PATH) $(LIBS) $(CFLAGS)

$(OUT_MASTER): $(COMMON_MASTER_OBJS) $(MASTER_OBJS)
	$(eval main_obj= $(addprefix $(objdir)/, $(addsuffix .o, $@)))
	$(CC) $(main_obj) $(COMMON_MASTER_OBJS) -o $@ $(INCLUDE_PATH) -L$(LIB_PATH) $(LIBS) $(CFLAGS)

define BUILD_SRC
@echo Compiling $<
$(CC) $(INCLUDE_PATH) $(CFLAGS) -c $< -o $@
endef

$(objdir)/%.o: $(common_dir)/%.cpp
	$(BUILD_SRC)
$(objdir)/%.o: $(worker_dir)/%.cpp
	$(BUILD_SRC)
$(objdir)/%.o: $(master_dir)/%.cpp
	$(BUILD_SRC)

define BUILD_DEPS
$(CC) -MM $< > $@ $(INCLUDE_PATH)
@sed -i "s/^/$(objdir)\//" $@
@cat $@ >> $(depdir)/.depend
endef

$(depdir)/%.d: $(common_dir)/%.cpp
	$(BUILD_DEPS)
$(depdir)/%.d: $(worker_dir)/%.cpp
	$(BUILD_DEPS)
$(depdir)/%.d: $(master_dir)/%.cpp
	$(BUILD_DEPS)

-include $(depdir)/.depend

clean:
	$(RM) $(OUT_WORKER) $(OUT_MASTER) $(objdir) $(depdir)


.PHONY: all clean installdirs debug
