.PHONY: dev run build senddev sendprod

dev:
	rm ./mt_manager; go build ; \
	export MOBILINK_RESPONSE_LOG=response.log; \
	export MOBILINK_REQUEST_LOG=response.log; \
	./mt_manager;

run:
	export MOBILINK_RESPONSE_LOG=response.log; \
  export MOBILINK_REQUEST_LOG=response.log; \
  ./mt_manager

build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/mt_manager-linux-amd64

pending:
	go build -ldflags "-s -w" pending_retries_parse_log.go;
	scp -i ~/.ssh/mobilink_centos.pem pending_retries_parse_log centos@52.66.23.201:/home/centos/linkit/pending_retries_parse_log


metrics:
	curl http://localhost:50305/debug/vars

cqrblacklist:
	curl http://localhost:50305/cqr?t=blacklist

cqrservices:
	curl http://localhost:50305/cqr?t=services

cqroperators:
	curl http://localhost:50305/cqr?t=operators
