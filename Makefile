.PHONY: format verify test validate
.SILENT: format verify test validate

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

validate: verify test
