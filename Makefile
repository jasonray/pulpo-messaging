all: default

clean: clean-output clean-test

clean-output: 
	rm -rf /tmp/kessel

clean-test: 
	rm -rf /tmp/kessel/unit-test/

deps:
	pip install -r requirements.txt

dev_deps:
	pip install -r requirements-dev.txt

check-format: dev_deps
	yapf -rd kessel

format: dev_deps
	yapf -ri kessel

lint: check-format
	pylint -r n kessel

lint-no-error: 
	pylint --exit-zero -r n kessel

test: clean-test build dev_deps
	python3 -m pytest -v --durations=0

build: deps
	# might re-add clean 
