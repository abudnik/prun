CC := g++
RFLAGS := -Wall -pedantic -pthread -O3
DFLAGS := -Wall -pedantic -pthread -g

ifneq ($(MAKECMDGOALS),debug)
	CFLAGS := $(RFLAGS)
else
	CFLAGS := $(DFLAGS)
endif

INCLUDE_PATH := -I/usr/include/boost -I/usr/include/python2.7

RM := rm -rf

LIBS := -lboost_system -lboost_thread -lboost_program_options -lboost_filesystem -lpython2.7 -lrt
LIB_PATH := /usr/lib

srcdir := src
objdir := objs
depdir := deps
OUT := PyServer PyExec PySender
OBJS := $(addprefix $(objdir)/, $(addsuffix .o, $(OUT)))
DEPENDS := $(addprefix $(depdir)/, $(addsuffix .d, $(OUT)))

all: installdirs $(DEPENDS) $(OUT)

debug: all

installdirs:
	@mkdir -p $(objdir) $(depdir)

$(OUT): $(OBJS)
	$(eval main_obj= $(addprefix $(objdir)/, $(addsuffix .o, $@)))
	$(CC) $(main_obj) -o $@ $(INCLUDE_PATH) -L$(LIB_PATH) $(LIBS) $(CFLAGS)

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
