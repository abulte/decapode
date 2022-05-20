.PHONY: deps install lint build publish test

deps:  ## Install dependencies
	python -m pip install --upgrade pip
	python -m pip install -r requirements.txt
	python -m pip install -r requirements_dev.txt

install:  ## Install the package
	python -m pip install -e .

lint:  ## Lint and static-check
	python -m flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	python -m flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

build:
	python setup.py bdist_wheel

publish:  ## Publish to PyPi
	pip install twine
	twine upload --username "${PYPI_USERNAME}" --password "${PYPI_PASSWORD}" dist/*.whl

test:  ## Run tests
	python -m pytest -ra
