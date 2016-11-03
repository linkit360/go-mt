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


senddev:
		ssh -i ~/.ssh/mobilink_centos_testing_stage.pem  centos@35.154.8.158 rm /home/centos/linkit/mt_manager-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem  bin/mt_manager-linux-amd64 centos@35.154.8.158:/home/centos/linkit/mt_manager-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem  dev/mt_manager.yml centos@35.154.8.158:/home/centos/linkit/

config:
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem  dev/mt_manager.yml centos@35.154.8.158:/home/centos/linkit/

sendprod:
		ssh -i ~/.ssh/mobilink_centos.pem centos@52.66.23.201    rm /home/centos/linkit/mt_manager-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem bin/mt_manager-linux-amd64  centos@52.66.23.201:/home/centos/linkit/mt_manager-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/mt_manager.prod.yml centos@52.66.23.201:/home/centos/linkit/

metrics:
	curl http://localhost:50305/debug/vars

cqrblacklist:
	curl http://localhost:50305/cqr?t=blacklist

cqrservices:
	curl http://localhost:50305/cqr?t=services

cqroperators:
	curl http://localhost:50305/cqr?t=operators
