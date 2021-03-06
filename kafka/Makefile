clean:
	-rm -rf build
	-rm -rf dist
	-rm -rf *.egg-info
	-rm -f tests/.coverage
	-docker rm $(docker ps -a | awk '{print $1}')
	-docker rmi `docker images -q --filter "dangling=true"`

build: clean
	python setup.py bdist_wheel --universal

uninstall:
	-pip uninstall -y log-processor

install: uninstall build
	pip install -U dist/*.whl

test: uninstall install
	cd tests && nosetests -v --with-coverage --cover-package=log_processor

images: build
	docker build -f FirewallDockerfile -t willnx/vlab-firewall-processor .
	docker build -f WebLogDockerfile -t willnx/vlab-weblog-processor .
	docker build -f WorkerLogDockerfile -t willnx/vlab-workerlog-processor .
	docker build -f DnsLogDockerfile -t willnx/vlab-dnslog-processor .

up: clean
	docker-compose up --abort-on-container-exit
