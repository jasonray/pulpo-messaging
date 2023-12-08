all: default

clean: clean-output clean-test
	rm -rf pulpo_messaging/__pycache__
	rm -rf pulpo_messaging/tests/__pycache__

clean-output: 
	rm -rf /tmp/pulpo

clean-test: 
	rm -rf /tmp/pulpo/unit-test/
	rm -rf .pytest_cache

deps:
	pip install -r requirements.txt

dev_deps:
	pip install -r requirements-dev.txt

check-format: dev_deps
	yapf -rd pulpo_messaging

format: dev_deps
	yapf -ri pulpo_messaging

lint: check-format
	pylint -r n pulpo_messaging

lint-no-error: 
	pylint --exit-zero -r n pulpo_messaging

test: clean-test build dev_deps
	python3 -m pytest -v --durations=0

build: deps
	# might re-add clean 
