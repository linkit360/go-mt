.PHONY: dev run release

dev:
	rm ./mt; go build ; ./mt;

run:
	./mt

release:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/mt-linux-amd64

send:
		scp -i ~/.ssh/mobilink_centos.pem bin/mt-linux-amd64 centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/mt/bin/mt-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/mt/dev/

config:
		scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/mt/dev/


cqrblacklist:
	curl http://localhost:50305/cqr?t=blacklist

cqrservices:
	curl http://localhost:50305/cqr?t=services

cqroperators:
	curl http://localhost:50305/cqr?t=operators
