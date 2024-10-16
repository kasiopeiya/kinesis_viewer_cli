test:
	TERM=unknown pytest --cov-report term-missing --cov=rich tests/ -vv
test-no-cov:
	TERM=unknown pytest tests/ -vv
format-check:
	black --check .
format:
	black .
typecheck:
	mypy -p kdv --no-incremental
typecheck-report:
	mypy -p kdv --html-report mypy_report
