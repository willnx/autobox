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
	-pip uninstall -y stat-collector

install: uninstall build
	pip install -U dist/*.whl

test: uninstall install
	cd tests && nosetests -v --with-coverage --cover-package=stat_collector

images: build
	docker build -f Dockerfile -t willnx/vlab-stat_collector .

up: clean
	docker-compose up --abort-on-container-exit
