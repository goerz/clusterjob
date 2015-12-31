PROJECT_NAME = clusterjob
PACKAGES =  pip click ipython sphinx coverage
TESTPYPI = https://testpypi.python.org/pypi

TESTOPTIONS = -s -x --doctest-modules
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

test27: .venv/py27/bin/py.test
	.venv/py27/bin/coverage run $< -v $(TESTOPTIONS) $(TESTS)
	.venv/py27/bin/coverage report -m

test33: .venv/py33/bin/py.test
	.venv/py33/bin/coverage run $< -v $(TESTOPTIONS) $(TESTS)
	.venv/py33/bin/coverage report -m

test34: .venv/py34/bin/py.test
	.venv/py34/bin/coverage run $< -v $(TESTOPTIONS) $(TESTS)
	.venv/py34/bin/coverage report -m

test: test27 test33 test34

doc: .venv/py34/bin/py.test
	$(MAKE) -C docs SPHINXBUILD=../.venv/py34/bin/sphinx-build SPHINXAPIDOC=../.venv/py34/bin/sphinx-apidoc html
	@ln -s docs/build/html doc

htmlcov/index.html: test34
	.venv/py34/bin/coverage html

.PHONY: install develop uninstall upload test-upload test-install sdist clean \
test test27 test33 test34 distclean help
