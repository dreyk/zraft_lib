REBAR = $(shell pwd)/rebar
.PHONY: rel deps test

all: deps compile

##
## Compilation targets
##

deps:
	$(REBAR) get-deps

compile: deps
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean
	$(REBAR) delete-deps

