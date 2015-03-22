install:
	pip install -Ie .

uninstall:
	pip uninstall clusterjob

sdist:
	python setup.py sdist

clean:
	@rm -rf build
	@rm -rf dist
	@rm -f *.pyc
	@rm -rf clusterjob.egg-info
	@rm -f clusterjob/*.pyc
	@rm -f clusterjob/backends/*.pyc
	@rm -f tests/*.pyc

test:
	python run_tests.py

.PHONY: install uninstall sdist clean
