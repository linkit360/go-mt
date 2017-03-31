.PHONY: dev run build senddev sendprod

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

dev:
	rm ./mt; go build ; \
	export MOBILINK_RESPONSE_LOG=response.log; \
	export MOBILINK_REQUEST_LOG=response.log; \
	./mt;

run:
	export MOBILINK_RESPONSE_LOG=response.log; \
  export MOBILINK_REQUEST_LOG=response.log; \
  ./mt

rm:
	rm bin/mt-linux-amd64; rm ~/linkit/mt-linux-amd64

build:
	export GOOS=linux; export GOARCH=amd64; \
	sed -i "s/%VERSION%/$(VERSION)/g" /home/centos/linkit360/go-utils/metrics/metrics.go; \
  go build -ldflags "-s -w" -o bin/mt-linux-amd64 ;

cp:
	cp bin/mt-linux-amd64 ~/linkit; cp dev/mt.yml  ~/linkit/;

metrics:
	curl http://localhost:50305/debug/vars

cqrblacklist:
	curl http://localhost:50305/cqr?t=blacklist

cqrservices:
	curl http://localhost:50305/cqr?t=services

cqroperators:
	curl http://localhost:50305/cqr?t=operators
