.PHONY: dev run build senddev sendprod

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

dev:
	rm ./mt_manager; go build ; \
	export MOBILINK_RESPONSE_LOG=response.log; \
	export MOBILINK_REQUEST_LOG=response.log; \
	./mt_manager;

run:
	export MOBILINK_RESPONSE_LOG=response.log; \
  export MOBILINK_REQUEST_LOG=response.log; \
  ./mt_manager

rm:
	rm bin/mt_manager-linux-amd64; rm ~/linkit/mt_manager-linux-amd64

build:
	export GOOS=linux; export GOARCH=amd64; \
	sed -i "s/%VERSION%/$(VERSION)/g" /home/centos/vostrok/utils/metrics/metrics.go; \
  go build -ldflags "-s -w" -o bin/mt_manager-linux-amd64 ; cp bin/mt_manager-linux-amd64 ~/linkit; cp dev/mt_manager.yml  ~/linkit/;

metrics:
	curl http://localhost:50305/debug/vars

cqrblacklist:
	curl http://localhost:50305/cqr?t=blacklist

cqrservices:
	curl http://localhost:50305/cqr?t=services

cqroperators:
	curl http://localhost:50305/cqr?t=operators
