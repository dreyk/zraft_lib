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

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

include tools.mk

typer:
	typer --annotate -I ../ --plt $(PLT) -r src

