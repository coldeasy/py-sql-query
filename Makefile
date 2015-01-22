.PHONY: docs

init:
	pip install -r requirements.txt

test:
	nosetests

ci: init
	nosetests --xunit-file=junit.xml

publish:
	python setup.py register sdist upload

build:
	python setup.py build

docs:
	cd docs && make html
