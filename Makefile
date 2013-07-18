CC := g++
RFLAGS := -Wall -pedantic -pthread -O3
DFLAGS := -Wall -pedantic -pthread -O0 -ggdb

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

COMMON := log config common
COMMON_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(COMMON)))

WORKER := pyexec pyserver
WORKER_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(WORKER)))

OUT := pyserver pyexec

ALL_CPP := $(COMMON) $(WORKER)
DEPENDS := $(addprefix $(depdir)/, $(addsuffix .d, $(ALL_CPP)))


all: installdirs $(DEPENDS) $(OUT)

debug: all

installdirs:
	@mkdir -p $(objdir) $(depdir)

$(OUT): $(COMMON_OBJS) $(WORKER_OBJS)
	$(eval main_obj= $(addprefix $(objdir)/, $(addsuffix .o, $@)))
	$(CC) $(main_obj) $(COMMON_OBJS) -o $@ $(INCLUDE_PATH) -L$(LIB_PATH) $(LIBS) $(CFLAGS)

define BUILD_SRC
@echo Compiling $<
$(CC) $(INCLUDE_PATH) $(CFLAGS) -c $< -o $@
endef

$(objdir)/%.o: $(common_dir)/%.cpp
	$(BUILD_SRC)
$(objdir)/%.o: $(worker_dir)/%.cpp
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

-include $(depdir)/.depend

clean:
	$(RM) $(OUT) $(objdir) $(depdir)


.PHONY: all clean installdirs debug
