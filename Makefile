test:
	python -m pytest test/ -s -vv --cov=src --cov-report term-missing
test-no-cov:
	python -m pytest test/ -s -vv
format-check:
	black --check .
format:
	black .
typecheck:
	mypy -p kdv --no-incremental
typecheck-report:
	mypy -p kdv --html-report mypy_report
