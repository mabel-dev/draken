lint:
	python -m pip install --quiet --upgrade pycln isort ruff yamllint cython-lint
#	python -m yamllint .
	cython-lint draken/**/*.pyx
	python -m ruff check --fix --exit-zero
	python -m pycln .
	python -m isort .
	python -m ruff format draken

update:
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade -r pyproject.toml
	#python -m uv pip install --upgrade -r pyproject.toml --extra=dev

coverage:
	python -m pip install --quiet --upgrade pytest coverage
	python -m coverage run -m pytest --color=yes
	python -m coverage report --include=orso/** --fail-under=60 -m

test:
	python -m pip install --quiet --upgrade pytest
	python -m pytest -n auto --color=yes

compile:
	clear
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade numpy 'cython==3.1.3' setuptools
	find . -name '*.so' -delete
	rm -rf build dist *.egg-info
	python setup.py clean
	python setup.py build_ext --inplace -j 8

c:
	python setup.py build_ext --inplace
