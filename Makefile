PROJECT_NAME = clusterjob
PACKAGES =  pip click ipython sphinx coverage
TESTPYPI = https://testpypi.python.org/pypi

TESTOPTIONS = -s -x --doctest-modules --cov=$(PROJECT_NAME)
TESTS = ${PROJECT_NAME} tests
# You may redefine TESTS to run a specific test. E.g.
#     make test TESTS="tests/test_io.py"

help:
	@echo "Please use \`make <target>'. Review the Makefile for available targets."

develop:
	pip install -e .[dev]

install:
	pip install .

uninstall:
	pip uninstall $(PROJECT_NAME)

sdist:
	python setup.py sdist

upload:
	python setup.py register
	python setup.py sdist upload

test-upload:
	python setup.py register -r $(TESTPYPI)
	python setup.py sdist upload -r $(TESTPYPI)

test-install:
	pip install -i $(TESTPYPI) $(PROJECT_NAME)

clean:
	@rm -rf build
	@rm -rf dist
	@rm -f *.pyc
	@rm -rf clusterjob.egg-info
	@rm -f clusterjob/*.pyc
	@rm -rf clusterjob/__pycache__
	@rm -rf clusterjob/backend/__pycache__
	@rm -f clusterjob/backends/*.pyc
	@rm -f tests/*.pyc
	@$(MAKE) -C docs clean
	@rm -f doc
	@rm -rf htmlcov

distclean: clean
	@rm -rf .venv

.venv/py27/bin/py.test:
	@conda create -y -m -p .venv/py27 python=2.7 $(PACKAGES)
	@.venv/py27/bin/pip install -e .[dev]

.venv/py33/bin/py.test:
	@conda create -y -m -p .venv/py33 python=3.3 $(PACKAGES)
	@.venv/py33/bin/pip install -e .[dev]

.venv/py34/bin/py.test:
	@conda create -y -m -p .venv/py34 python=3.4 $(PACKAGES)
	@.venv/py34/bin/pip install -e .[dev]

.venv/py35/bin/py.test:
	@conda create -y -m -p .venv/py35 python=3.5 $(PACKAGES)
	@.venv/py35/bin/pip install -e .[dev]

test27: .venv/py27/bin/py.test
	$< -v $(TESTOPTIONS) $(TESTS)

test33: .venv/py33/bin/py.test
	$< -v $(TESTOPTIONS) $(TESTS)

test34: .venv/py34/bin/py.test
	$< -v $(TESTOPTIONS) $(TESTS)

test35: .venv/py35/bin/py.test
	$< -v $(TESTOPTIONS) $(TESTS)

test: test27 test33 test34 test35

doc: .venv/py35/bin/py.test
	@rm docs/source/API/clusterjob.*
	$(MAKE) -C docs SPHINXBUILD=../.venv/py35/bin/sphinx-build SPHINXAPIDOC=../.venv/py35/bin/sphinx-apidoc html
	@ln -s docs/build/html doc

coverage: test35
	@rm -rf htmlcov/index.html
	.venv/py35/bin/coverage html

.PHONY: install develop uninstall upload test-upload test-install sdist clean \
test test27 test33 test34 test35 distclean help coverage
