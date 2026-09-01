# NIF for the GRAFCET static analyser. Links against
# ../taskweft-grafcet-static/libgrafcet_static.{dylib,so}, which is the
# Lean-produced analyser + C bridge from RFD 2144.

ERL_INCLUDE := $(shell erl -eval 'io:format("~ts", [code:root_dir()])' -s init stop -noshell)/erts-$(shell erl -eval 'io:format("~ts", [erlang:system_info(version)])' -s init stop -noshell)/include

STATIC_ROOT := ../taskweft-grafcet-static
STATIC_LIB  := $(STATIC_ROOT)/libgrafcet_static.dylib

CXX     := c++
CXXFLAGS := -std=c++17 -O2 -fPIC -Wall -I$(ERL_INCLUDE)

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    LDFLAGS := -shared -undefined dynamic_lookup -Wl,-rpath,@loader_path
    SO_EXT := so
else
    LDFLAGS := -shared -Wl,-rpath,\$$ORIGIN
    SO_EXT := so
endif

TARGET := priv/grafcet_static_nif.$(SO_EXT)
PRIV_LIB := priv/libgrafcet_static.dylib

all: $(TARGET) $(PRIV_LIB)

$(STATIC_LIB):
	$(MAKE) -C $(STATIC_ROOT)

$(TARGET): c_src/grafcet_static_nif.cpp $(STATIC_LIB)
	@mkdir -p priv
	$(CXX) $(CXXFLAGS) c_src/grafcet_static_nif.cpp \
	  -L$(STATIC_ROOT) -lgrafcet_static \
	  $(LDFLAGS) -o $@

$(PRIV_LIB): $(STATIC_LIB)
	@mkdir -p priv
	cp $(STATIC_LIB) $(PRIV_LIB)

clean:
	rm -f $(TARGET) $(PRIV_LIB)

.PHONY: all clean
