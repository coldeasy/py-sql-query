.PHONY: docs

init:
	pip install -r requirements.txt

test:
	nosetests

ci: init
	nosetests --xunit-file=junit.xml

publish:
	python setup.py register
	python setup.py sdist upload
	python setup.py bdist_wheel upload


docs:
	cd docs && make html
