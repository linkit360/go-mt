.PHONY: dev run build senddev sendprod

dev:
	rm ./mt; go build ; \
	export MOBILINK_RESPONSE_LOG=response.log; \
	export MOBILINK_REQUEST_LOG=response.log; \
	./mt;

run:
	export MOBILINK_RESPONSE_LOG=response.log; \
  export MOBILINK_REQUEST_LOG=response.log; \
  ./mt

build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/mt-linux-amd64


sendprod:
		ssh -i ~/.ssh/mobilink_centos.pem centos@52.66.23.201    rm /home/centos/go/src/github.com/vostrok/mt/bin/mt-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem bin/mt-linux-amd64     centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/mt/bin/mt-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.prod.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/mt/dev/

senddev:
		ssh -i ~/.ssh/mobilink_centos_testing_stage.pem  centos@35.154.8.158 rm /home/centos/go/src/github.com/vostrok/mt/bin/mt-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem  bin/mt-linux-amd64 centos@35.154.8.158:/home/centos/go/src/github.com/vostrok/mt/bin/mt-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem  dev/appconfig.yml centos@35.154.8.158:/home/centos/go/src/github.com/vostrok/mt/dev/


metrics:
	curl http://localhost:50305/debug/vars

cqrblacklist:
	curl http://localhost:50305/cqr?t=blacklist

cqrservices:
	curl http://localhost:50305/cqr?t=services

cqroperators:
	curl http://localhost:50305/cqr?t=operators
