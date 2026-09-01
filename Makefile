.PHONY: format verify test
.SILENT: format verify test

PY      := uv run
RUFF    := $(PY) ruff
PYTEST  := $(PY) pytest
PYRIGHT := $(PY) pyright

format:
	$(RUFF) check --fix .
	$(RUFF) format .

verify:
	$(RUFF) check .
	$(RUFF) format --check .
	$(PYRIGHT)

test:
	$(PYTEST)
