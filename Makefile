usr=$(USER)
install: python3-7 virtualenv pgdeps python3deps
	. venv/bin/activate; \
	pip install -r requirements.txt

install-dev: python3-7 virtualenv pgdeps python3deps
	. venv/bin/activate; \
	pip install -r requirements-dev.txt

pgdeps:
	sudo apt-get install libpq-dev

python3deps:
	sudo apt-get update; \
	sudo apt-get install python3.7 python-dev python3.7-dev build-essential libssl-dev libffi-dev libxml2-dev libxslt1-dev zlib1g-dev python-pip


python3-7:
	python3.7 --version || sudo apt update; \
	python3.7 --version || sudo apt install software-properties-common; \
	python3.7 --version || sudo add-apt-repository ppa:deadsnakes/ppa; \
	python3.7 --version || sudo apt install python3.7; \


virtualenv:
	virtualenv --version || sudo apt install virtualenv; \
	test -d venv || virtualenv venv -p python3.7