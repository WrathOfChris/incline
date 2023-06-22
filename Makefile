all: install
	@echo done

build:
	python3 -m build --wheel

test:
	AWS_PROFILE=test-incline pytest

test-telemetry:
	AWS_PROFILE=test-incline pytest --capture=no

depend:
	python3 -m pip install --requirement requirements.txt

depend-dev:
	python3 -m pip install --requirement requirements-dev.txt
	python3 -m pip install -e ".[dev]"

install:
	python -m setup install

incline/VERSION:
	setuptools-git-versioning >$@

opentelemetry:
	opentelemetry-bootstrap -a install

lint:
	yapf --in-place --verbose --recursive incline/ tests/

typecheck:
	mypy --non-interactive \
		--install-types \
		--config-file mypy.ini \
		incline/ bin/ tests/

clean:
	rm -rf incline.egg-info
	rm -rf dist
	find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

.PHONY: all build test depend depend-dev install clean lint
