CC := g++
RFLAGS := -Wall -pedantic -pthread -O3
DFLAGS := -Wall -pedantic -pthread -ggdb -O0

ifneq ($(MAKECMDGOALS),debug)
	CFLAGS := $(RFLAGS)
else
	CFLAGS := $(DFLAGS)
endif

INCLUDE_PATH := -I/usr/include/boost

RM := rm -rf

LIBS := -lboost_system -lboost_thread -lboost_program_options -lboost_filesystem -lrt
LIB_PATH := /usr/lib

srcdir := src
objdir := objs
depdir := deps
OUT := pyserver pyexec pysender
OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(OUT)))
COMMON := common log config
COMMON_OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(COMMON)))
ALL_CPP := $(COMMON) $(OUT)
DEPENDS := $(addprefix $(depdir)/, $(addsuffix .d, $(ALL_CPP)))

all: installdirs $(DEPENDS) $(OUT)

debug: all

installdirs:
	@mkdir -p $(objdir) $(depdir)

$(OUT): $(COMMON_OBJS) $(OBJS)
	$(eval main_obj= $(addprefix $(objdir)/, $(addsuffix .o, $@)))
	$(CC) $(main_obj) $(COMMON_OBJS) -o $@ $(INCLUDE_PATH) -L$(LIB_PATH) $(LIBS) $(CFLAGS)

$(objdir)/%.o: $(srcdir)/%.cpp
	@echo Compiling $<
	$(CC) $(INCLUDE_PATH) $(CFLAGS) -c $< -o $@

$(depdir)/%.d: $(srcdir)/%.cpp
	$(CC) -MM $< > $@
	@sed -i "s/^/$(objdir)\//" $@
	@cat $@ >> $(depdir)/.depend

-include $(depdir)/.depend

clean:
	$(RM) $(OUT) $(objdir) $(depdir)


.PHONY: all clean installdirs debug
