CC      ?= gcc
CFLAGS  ?= -g
CPPFLAGS := -I include
#SCHEDIFLAGS := -DLOCKLESS_READYJOB
LDFLAGS := -pthread

SRC := $(wildcard src/*.c)
OBJ := $(SRC:src/%.c=build/%.o)
EXAMPLES := $(wildcard examples/*.c)
EXECUTABLES := $(patsubst examples/%.c,bin/%,$(EXAMPLES))

all: lib $(EXECUTABLES)

lib: libschedi.a

libschedi.a: $(OBJ)
	ar rcs $@ $^

build/%.o: src/%.c | build
	$(CC) $(CFLAGS) $(CPPFLAGS) $(SCHEDIFLAGS) -c -o $@ $<

build:
	mkdir -p build

bin/%: examples/%.c libschedi.a | bin
	$(CC) $(CFLAGS) $(CPPFLAGS) $(SCHEDIFLAGS) -o $@ $< libschedi.a $(LDFLAGS)

bin:
	mkdir -p bin

clean:
	rm -rf bin build libschedi.a

.PHONY: all clean lib
